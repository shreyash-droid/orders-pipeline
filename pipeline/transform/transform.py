"""TRANSFORM: raw zone -> curated zone, with Spark.

    python -m pipeline.transform.transform --date 2026-10-01

The logic functions take DataFrames and return DataFrames. They never read or
write files, which is what makes them unit-testable (step 13).
"""
import argparse
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline import lake
from pipeline.transform.spark_session import get_spark

ORDER_LINE_COLUMNS = ["order_item_id", "order_id", "customer_id", "product_id", "order_ts", "order_date",
                      "status", "quantity", "unit_price", "line_amount", "order_updated_at"]


def latest_per_key(df, key, order_col):
    """Deduplicate: keep the most recent row for each key (a WIDE transformation: shuffles by key)."""
    newest_first = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(newest_first)).filter(F.col("_rn") == 1).drop("_rn")


def build_order_lines(orders, order_items):
    """Join order headers onto their lines. Output grain: one row per order line."""
    orders = latest_per_key(orders, "order_id", "updated_at").select(
        "order_id", "customer_id", "order_ts", "status", F.col("updated_at").alias("order_updated_at"))
    order_items = order_items.dropDuplicates(["order_item_id"])
    return (
        order_items
        .join(F.broadcast(orders), on="order_id", how="inner")   # small side copied to every executor
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
        .select(*ORDER_LINE_COLUMNS)
    )


def split_valid(order_lines):
    """Separate good rows from bad. Bad rows are quarantined, never silently dropped."""
    is_valid = F.coalesce(F.col("quantity") > 0, F.lit(False))   # a NULL quantity counts as invalid too
    return order_lines.filter(is_valid), order_lines.filter(~is_valid)


def run(run_date):
    spark = get_spark()

    def raw(table):
        return spark.read.parquet(str(lake.partition_dir("raw", table, run_date)))

    def write(df, zone, table):
        target = str(lake.partition_dir(zone, table, run_date))
        # overwrite = idempotent. coalesce(1) = one output file because the data is tiny;
        # on real volumes you'd aim for files of roughly 128 MB to 1 GB each.
        df.coalesce(1).write.mode("overwrite").parquet(target)

    customers = latest_per_key(raw("customers"), "customer_id", "updated_at").select(
        "customer_id", "name", "email", "city", "segment", "updated_at")
    products = latest_per_key(raw("products"), "product_id", "updated_at").select(
        "product_id", "name", "category", "price", "updated_at")

    # Nothing has executed yet: these are just plans. cache() because order_lines feeds 4 actions.
    order_lines = build_order_lines(raw("orders"), raw("order_items")).cache()
    valid, rejected = split_valid(order_lines)

    counts = {"customers": customers.count(), "products": products.count(),
              "order_lines": valid.count(), "rejected_order_lines": rejected.count()}

    write(customers, "curated", "customers")
    write(products, "curated", "products")
    write(valid, "curated", "order_lines")
    write(rejected, "rejected", "order_lines")

    for name, count in counts.items():
        print(f"  transform {name:<21} {count:>6} rows")
    order_lines.unpersist()
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    run(parser.parse_args().date)