"""Pretend the shop was open for one day: write that day's activity into the OLTP database.

    python -m pipeline.source.simulate_day --date 2026-10-01
"""
import argparse
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from pipeline.db import connect_source
from pipeline.source.catalog import CITIES, random_customer


def run(day, orders=300):
    start = datetime.combine(day, time())
    end = start + timedelta(days=1)
    rng = random.Random(day.toordinal())  # same date -> same data

    def moment():
        """A random timestamp inside this day."""
        return start + timedelta(seconds=rng.randrange(86400))

    with connect_source() as conn:
        # Guard: never simulate the same day twice
        already = conn.execute(
            "SELECT count(*) FROM orders WHERE order_ts >= %s AND order_ts < %s", (start, end)
        ).fetchone()[0]
        if already:
            print(f"{day} was already simulated ({already} orders). Nothing to do.")
            return

        first_day = conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0
        moved = signed_up = repriced = shipped = cancelled = 0

        if not first_day:
            # 1. ~5% of customers move to a different city
            customers = conn.execute("SELECT customer_id, city FROM customers ORDER BY customer_id").fetchall()
            for customer_id, city in rng.sample(customers, k=max(1, len(customers) // 20)):
                new_city = rng.choice([c for c in CITIES if c != city])
                conn.execute("UPDATE customers SET city = %s, updated_at = %s WHERE customer_id = %s",
                             (new_city, moment(), customer_id))
                moved += 1

            # 2. 5 new customers sign up
            for _ in range(5):
                at = moment()
                conn.execute(
                    "INSERT INTO customers (name, email, city, segment, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s)", (*random_customer(rng), at, at))
                signed_up += 1

            # 3. 3 products change price by +/-10%
            products = conn.execute("SELECT product_id, price FROM products ORDER BY product_id").fetchall()
            for product_id, price in rng.sample(products, k=3):
                new_price = (price * Decimal(rng.choice(["0.90", "1.10"]))).quantize(Decimal("0.01"))
                conn.execute("UPDATE products SET price = %s, updated_at = %s WHERE product_id = %s",
                             (new_price, moment(), product_id))
                repriced += 1

            # 4. Older 'placed' orders ship (80%) or cancel (5%); the rest stay placed
            open_orders = conn.execute(
                "SELECT order_id FROM orders WHERE status = 'placed' AND order_ts < %s ORDER BY order_id",
                (start,)).fetchall()
            for (order_id,) in open_orders:
                roll = rng.random()
                status = "shipped" if roll < 0.80 else "cancelled" if roll < 0.85 else None
                if status:
                    conn.execute("UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                                 (status, moment(), order_id))
                    shipped += status == "shipped"
                    cancelled += status == "cancelled"

        # 5. New orders. A customer who signed up today can only order after signing up.
        customers = conn.execute("SELECT customer_id, created_at FROM customers ORDER BY customer_id").fetchall()
        products = conn.execute("SELECT product_id, price FROM products ORDER BY product_id").fetchall()
        lines = 0
        for _ in range(orders):
            customer_id, created_at = rng.choice(customers)
            placed_at = max(moment(), created_at)
            order_id = conn.execute(
                "INSERT INTO orders (customer_id, order_ts, status, updated_at) "
                "VALUES (%s, %s, 'placed', %s) RETURNING order_id",
                (customer_id, placed_at, placed_at)).fetchone()[0]
            for product_id, price in rng.sample(products, k=rng.randint(1, 4)):
                quantity = 0 if rng.random() < 0.01 else rng.randint(1, 3)   # the 1% app bug
                conn.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s)", (order_id, product_id, quantity, price))
                lines += 1

    print(f"{day}: {orders} new orders ({lines} lines), {moved} customers moved, {signed_up} signed up, "
          f"{repriced} prices changed, {shipped} orders shipped, {cancelled} cancelled.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--orders", type=int, default=300)
    args = parser.parse_args()
    run(args.date, args.orders)