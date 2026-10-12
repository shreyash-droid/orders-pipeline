"""The data lake: Parquet files in a folder layout that mirrors an S3 bucket.

    lake/<zone>/<table>/extract_date=YYYY-MM-DD/<files>.parquet
"""
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config


def partition_dir(zone, table, run_date):
    return config.LAKE_PATH / zone / table / f"extract_date={run_date.isoformat()}"


def write_rows(rows, schema, zone, table, run_date):
    """Write a list of dicts as one Parquet file, REPLACING the partition (idempotent)."""
    target = partition_dir(zone, table, run_date)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    table_data = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table_data, target / "data.parquet", coerce_timestamps="us")
    return target


def read_rows(zone, table, run_date):
    """Read every Parquet file in a partition back as a list of dicts."""
    source = partition_dir(zone, table, run_date)
    files = sorted(source.glob("*.parquet"))   # skips Spark's _SUCCESS and .crc files
    if not files:
        raise FileNotFoundError(f"No Parquet files in {source} - has the previous step run?")
    rows = []
    for file in files:
        rows.extend(pq.read_table(file).to_pylist())
    return rows