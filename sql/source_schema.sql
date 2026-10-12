-- OLTP source: the app's database (Amazon RDS on AWS).

CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name        TEXT      NOT NULL,
    email       TEXT      NOT NULL,
    city        TEXT      NOT NULL,
    segment     TEXT      NOT NULL,      -- consumer / student / business
    created_at  TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id  SERIAL PRIMARY KEY,
    name        TEXT           NOT NULL,
    category    TEXT           NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    updated_at  TIMESTAMP      NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INT       NOT NULL REFERENCES customers (customer_id),
    order_ts    TIMESTAMP NOT NULL,
    status      TEXT      NOT NULL,      -- placed -> shipped | cancelled
    updated_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT            NOT NULL REFERENCES orders (order_id),
    product_id    INT            NOT NULL REFERENCES products (product_id),
    quantity      INT            NOT NULL,   -- no CHECK (quantity > 0) on purpose: the "app" has a bug
    unit_price    NUMERIC(10, 2) NOT NULL    -- price at the moment of sale
);

CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers (updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated_at  ON products (updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at    ON orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id);