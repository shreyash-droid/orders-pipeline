"""DATA QUALITY: assertions that run after every load. Any failure stops the pipeline.

    python -m pipeline.quality.checks

Each check is a query that counts VIOLATIONS, so the expected answer is always 0.
"""
from pipeline.db import connect_source, connect_warehouse

CHECKS = [
    ("fact has no duplicate order lines (grain is unique)",
     "SELECT count(*) FROM (SELECT order_item_id FROM dw.fact_order_line "
     "GROUP BY order_item_id HAVING count(*) > 1) d"),

    ("every staged order line reached the fact table (no rows lost in the key lookups)",
     "SELECT count(*) FROM staging.order_lines s "
     "WHERE NOT EXISTS (SELECT 1 FROM dw.fact_order_line f WHERE f.order_item_id = s.order_item_id)"),

    ("every customer has exactly one current version",
     "SELECT count(*) FROM (SELECT customer_id FROM dw.dim_customer "
     "GROUP BY customer_id HAVING count(*) FILTER (WHERE is_current) <> 1) d"),

    ("customer versions do not overlap in time",
     "SELECT count(*) FROM dw.dim_customer a JOIN dw.dim_customer b "
     "ON a.customer_id = b.customer_id AND a.customer_key < b.customer_key "
     "AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to"),

    ("line_amount equals quantity x unit_price",
     "SELECT count(*) FROM dw.fact_order_line WHERE line_amount <> quantity * unit_price"),

    ("no zero or negative quantities in the fact table",
     "SELECT count(*) FROM dw.fact_order_line WHERE quantity <= 0"),

    ("order status is one of the known values",
     "SELECT count(*) FROM dw.fact_order_line WHERE status NOT IN ('placed', 'shipped', 'cancelled')"),
]

def reconcile():
    """Source-to-target: the two ends of the pipeline must agree on line count and revenue."""
    with connect_source() as src:
        src_lines, src_revenue = src.execute(
            "SELECT count(*), coalesce(sum(quantity * unit_price), 0) FROM order_items WHERE quantity > 0"
        ).fetchone()
    with connect_warehouse() as wh:
        wh_lines, wh_revenue = wh.execute(
            "SELECT count(*), coalesce(sum(line_amount), 0) FROM dw.fact_order_line"
        ).fetchone()
    ok = (src_lines, src_revenue) == (wh_lines, wh_revenue)
    print(f"  quality  {'PASS' if ok else 'FAIL'}  source vs warehouse: "
          f"lines {src_lines} vs {wh_lines}, revenue {src_revenue} vs {wh_revenue}")
    return ok

def run():
    failures = []
    with connect_warehouse() as conn:
        for name, query in CHECKS:
            violations = conn.execute(query).fetchone()[0]
            print(f"  quality  {'PASS' if violations == 0 else 'FAIL'}  {name}"
                  + (f"  ({violations} violations)" if violations else ""))
            if violations:
                failures.append(name)
    if not reconcile():
        failures.append("source-to-target reconciliation") 
    if failures:
        raise RuntimeError(f"{len(failures)} data quality check(s) failed: {failures}")


if __name__ == "__main__":
    run()