"""LOAD: curated zone -> warehouse star schema.

    python -m pipeline.load.load --date 2026-10-01

1. bulk-COPY curated Parquet rows into staging tables
2. run the SQL files in sql/load/ (in name order) to build dimensions and facts
All in ONE transaction: any failure leaves the warehouse exactly as it was.
"""
import argparse
from datetime import date

from pipeline import config, lake
from pipeline.db import connect_warehouse, sql_file

STAGING_COLUMNS = {
    "customers": ["customer_id", "name", "email", "city", "segment", "updated_at"],
    "products": ["product_id", "name", "category", "price", "updated_at"],
    "order_lines": ["order_item_id", "order_id", "customer_id", "product_id", "order_ts", "order_date",
                    "status", "quantity", "unit_price", "line_amount", "order_updated_at"],
}


def stage(conn, table, rows):
    """Replace a staging table's contents using COPY (far faster than row-by-row INSERTs)."""
    columns = STAGING_COLUMNS[table]
    conn.execute(f"TRUNCATE staging.{table}")
    with conn.cursor().copy(f"COPY staging.{table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row([row[column] for column in columns])


def run(run_date):
    counts = {}
    with connect_warehouse() as conn:
        conn.execute(sql_file("warehouse_schema.sql"))     # safe to re-run (IF NOT EXISTS)

        for table in STAGING_COLUMNS:
            rows = lake.read_rows("curated", table, run_date)
            stage(conn, table, rows)
            counts[table] = len(rows)
            print(f"  load     staging.{table:<12} {len(rows):>6} rows")

        # Dimensions first, then facts: a fact row needs its dimension keys to exist.
        for script in sorted((config.SQL_DIR / "load").glob("*.sql")):
            conn.execute(script.read_text(encoding="utf-8"))
            print(f"  load     ran {script.name}")

        conn.execute(
            "INSERT INTO dw.pipeline_runs (run_date, customers, products, order_lines) VALUES (%s, %s, %s, %s)",
            (run_date, counts["customers"], counts["products"], counts["order_lines"]))
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    run(parser.parse_args().date)