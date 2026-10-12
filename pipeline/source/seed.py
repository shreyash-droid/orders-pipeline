"""Create the OLTP schema and the starting customers and products.

    python -m pipeline.source.seed --date 2026-10-01
    python -m pipeline.source.seed --date 2026-10-01 --reset
"""
import argparse
import random
from datetime import date, datetime, time

from pipeline.db import connect_source, sql_file
from pipeline.source.catalog import product_rows, random_customer


def run(start_date, customers=200, reset=False):
    created = datetime.combine(start_date, time())   # midnight on day one
    rng = random.Random(42)                          # fixed seed -> same data every time

    with connect_source() as conn:                   # leaving this block = COMMIT; an error = ROLLBACK
        if reset:
            conn.execute("DROP TABLE IF EXISTS order_items, orders, products, customers CASCADE")
        conn.execute(sql_file("source_schema.sql"))

        if conn.execute("SELECT count(*) FROM customers").fetchone()[0] > 0:
            print("Source is already seeded. Use --reset to start again.")
            return

        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO customers (name, email, city, segment, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            [(*random_customer(rng), created, created) for _ in range(customers)],
        )
        products = product_rows()
        cur.executemany(
            "INSERT INTO products (name, category, price, updated_at) VALUES (%s, %s, %s, %s)",
            [(name, category, price, created) for name, category, price in products],
        )
    print(f"Seeded {customers} customers and {len(products)} products as of {start_date}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    run(args.date, args.customers, args.reset)