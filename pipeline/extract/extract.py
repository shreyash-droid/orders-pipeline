"""EXTRACT: copy one day's changes from the OLTP database into the raw zone of the lake.

    python -m pipeline.extract.extract --date 2026-10-01
"""
import argparse
from datetime import date, datetime, time, timedelta

import pyarrow as pa

from pipeline import lake
from pipeline.db import connect_source

TS = pa.timestamp("us")
MONEY = pa.decimal128(10, 2)

# For each source table: an explicit schema (the "contract" of the raw zone)
# and the query that selects rows changed inside the window.
TABLES = {
    "customers": {
        "schema": pa.schema([("customer_id", pa.int64()), ("name", pa.string()), ("email", pa.string()),
                             ("city", pa.string()), ("segment", pa.string()),
                             ("created_at", TS), ("updated_at", TS)]),
        "query": "SELECT customer_id, name, email, city, segment, created_at, updated_at "
                 "FROM customers WHERE updated_at >= %s AND updated_at < %s",
    },
    "products": {
        "schema": pa.schema([("product_id", pa.int64()), ("name", pa.string()), ("category", pa.string()),
                             ("price", MONEY), ("updated_at", TS)]),
        "query": "SELECT product_id, name, category, price, updated_at "
                 "FROM products WHERE updated_at >= %s AND updated_at < %s",
    },
    "orders": {
        "schema": pa.schema([("order_id", pa.int64()), ("customer_id", pa.int64()), ("order_ts", TS),
                             ("status", pa.string()), ("updated_at", TS)]),
        "query": "SELECT order_id, customer_id, order_ts, status, updated_at "
                 "FROM orders WHERE updated_at >= %s AND updated_at < %s",
    },
    # order_items has no updated_at, so it rides on its parent order:
    # whenever an order changes, all of its lines are extracted again.
    "order_items": {
        "schema": pa.schema([("order_item_id", pa.int64()), ("order_id", pa.int64()),
                             ("product_id", pa.int64()), ("quantity", pa.int32()), ("unit_price", MONEY)]),
        "query": "SELECT i.order_item_id, i.order_id, i.product_id, i.quantity, i.unit_price "
                 "FROM order_items i JOIN orders o ON o.order_id = i.order_id "
                 "WHERE o.updated_at >= %s AND o.updated_at < %s",
    },
}


def run(run_date):
    window_start = datetime.combine(run_date, time())
    window_end = window_start + timedelta(days=1)
    counts = {}

    with connect_source() as conn:
        for table, spec in TABLES.items():
            cur = conn.execute(spec["query"], (window_start, window_end))
            columns = [column.name for column in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            target = lake.write_rows(rows, spec["schema"], "raw", table, run_date)
            counts[table] = len(rows)
            print(f"  extract  {table:<12} {len(rows):>6} rows -> {target}")
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    run(parser.parse_args().date)