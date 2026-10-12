-- Warehouse (Amazon Redshift on AWS). Safe to run repeatedly.
--   staging.* = today's batch, truncated and reloaded on every run
--   dw.*      = the star schema analysts query

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

-- ---------- staging: same shape as the curated Parquet files ----------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id BIGINT, name TEXT, email TEXT, city TEXT, segment TEXT, updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS staging.products (
    product_id BIGINT, name TEXT, category TEXT, price NUMERIC(10, 2), updated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS staging.order_lines (
    order_item_id BIGINT, order_id BIGINT, customer_id BIGINT, product_id BIGINT,
    order_ts TIMESTAMP, order_date DATE, status TEXT,
    quantity INT, unit_price NUMERIC(10, 2), line_amount NUMERIC(12, 2),
    order_updated_at TIMESTAMP
);

-- ---------- dimensions ----------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key   INT PRIMARY KEY,          -- 20261001
    full_date  DATE NOT NULL UNIQUE,
    year       INT  NOT NULL,
    month      INT  NOT NULL,
    month_name TEXT NOT NULL,
    day        INT  NOT NULL,
    day_name   TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- SCD Type 1: one row per product, overwritten in place.
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- surrogate key
    product_id  BIGINT NOT NULL UNIQUE,                            -- business key
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,                           -- CURRENT list price
    updated_at  TIMESTAMP NOT NULL
);

-- SCD Type 2: one row per VERSION of a customer.
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- one per version
    customer_id  BIGINT NOT NULL,                                  -- repeats across versions
    name         TEXT NOT NULL,
    email        TEXT NOT NULL,
    city         TEXT NOT NULL,
    segment      TEXT NOT NULL,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP NOT NULL,                               -- 9999-12-31 = still current
    is_current   BOOLEAN   NOT NULL
);
-- Exactly one current version per customer (Redshift would NOT enforce this -> quality checks later)
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current
    ON dw.dim_customer (customer_id) WHERE is_current;

-- ---------- fact ----------
-- GRAIN: one row per order line.
CREATE TABLE IF NOT EXISTS dw.fact_order_line (
    order_item_id BIGINT PRIMARY KEY,
    order_id      BIGINT NOT NULL,
    customer_key  BIGINT NOT NULL REFERENCES dw.dim_customer (customer_key),
    product_key   BIGINT NOT NULL REFERENCES dw.dim_product (product_key),
    date_key      INT    NOT NULL REFERENCES dw.dim_date (date_key),
    order_ts      TIMESTAMP NOT NULL,
    status        TEXT   NOT NULL,
    quantity      INT    NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL,
    line_amount   NUMERIC(12, 2) NOT NULL,
    loaded_at     TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_date ON dw.fact_order_line (date_key);

-- ---------- run log ----------
CREATE TABLE IF NOT EXISTS dw.pipeline_runs (
    run_date    DATE NOT NULL,
    finished_at TIMESTAMP NOT NULL DEFAULT now(),
    customers   INT NOT NULL,
    products    INT NOT NULL,
    order_lines INT NOT NULL
);

-- ---------- dim_date is generated, not extracted ----------
INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::INT, d::DATE,
       extract(year FROM d)::INT, extract(month FROM d)::INT, trim(to_char(d, 'Month')),
       extract(day FROM d)::INT, trim(to_char(d, 'Day')),
       extract(isodow FROM d) IN (6, 7)
FROM generate_series(DATE '2026-01-01', DATE '2027-12-31', INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- Version of the source order each fact row came from (lets older batches never overwrite newer ones).
ALTER TABLE dw.fact_order_line ADD COLUMN IF NOT EXISTS order_updated_at TIMESTAMP;