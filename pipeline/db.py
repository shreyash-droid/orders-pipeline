import psycopg

from pipeline import config


def connect_source():
    return psycopg.connect(config.SOURCE_DSN)


def connect_warehouse():
    return psycopg.connect(config.WAREHOUSE_DSN)


def sql_file(relative_path):
    return (config.SQL_DIR / relative_path).read_text(encoding="utf-8")