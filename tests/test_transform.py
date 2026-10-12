"""Unit tests for the Spark logic. Run with:  pytest -q"""
from datetime import date, datetime
from decimal import Decimal

import pytest

from pipeline import lake
from pipeline.transform.spark_session import get_spark
from pipeline.transform.transform import build_order_lines, latest_per_key, split_valid

ORDERS_SCHEMA = "order_id long, customer_id long, order_ts timestamp_ntz, status string, updated_at timestamp_ntz"
ITEMS_SCHEMA = "order_item_id long, order_id long, product_id long, quantity int, unit_price decimal(10,2)"


@pytest.fixture(scope="session")
def spark():
    session = get_spark("tests")       # one Spark session shared by all tests (startup is slow)
    yield session
    session.stop()


def test_latest_per_key_keeps_newest_row(spark):
    df = spark.createDataFrame(
        [(1, "placed", datetime(2026, 10, 1, 9)), (1, "shipped", datetime(2026, 10, 1, 18)),
         (2, "placed", datetime(2026, 10, 1, 10))],
        "order_id long, status string, updated_at timestamp_ntz")

    result = {row.order_id: row.status for row in latest_per_key(df, "order_id", "updated_at").collect()}

    assert result == {1: "shipped", 2: "placed"}


def test_build_order_lines_one_row_per_line_and_computes_amount(spark):
    placed = datetime(2026, 10, 1, 9)
    orders = spark.createDataFrame([(10, 7, placed, "placed", placed)], ORDERS_SCHEMA)
    items = spark.createDataFrame(
        [(100, 10, 1, 2, Decimal("199.00")),
         (101, 10, 2, 1, Decimal("50.50")),
         (101, 10, 2, 1, Decimal("50.50"))],          # duplicate line: must be removed
        ITEMS_SCHEMA)

    rows = {row.order_item_id: row for row in build_order_lines(orders, items).collect()}

    assert set(rows) == {100, 101}
    assert rows[100].line_amount == Decimal("398.00")
    assert rows[100].customer_id == 7
    assert rows[100].order_date == date(2026, 10, 1)


def test_split_valid_quarantines_zero_quantity(spark):
    placed = datetime(2026, 10, 1, 9)
    orders = spark.createDataFrame([(10, 7, placed, "placed", placed)], ORDERS_SCHEMA)
    items = spark.createDataFrame(
        [(100, 10, 1, 2, Decimal("10.00")), (101, 10, 2, 0, Decimal("10.00"))], ITEMS_SCHEMA)

    valid, rejected = split_valid(build_order_lines(orders, items))

    assert [row.order_item_id for row in valid.collect()] == [100]
    assert [row.order_item_id for row in rejected.collect()] == [101]


def test_partition_dir_uses_hive_style_layout():
    path = lake.partition_dir("raw", "orders", date(2026, 10, 1))
    assert path.parts[-3:] == ("raw", "orders", "extract_date=2026-10-01")