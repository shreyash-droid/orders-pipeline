-- Idempotent, version-aware fact load on the grain's business key (order_item_id).

-- 1. Remove existing rows that the staged batch supersedes (same or newer version).
DELETE FROM dw.fact_order_line AS f
USING staging.order_lines AS s
WHERE f.order_item_id = s.order_item_id
  AND f.order_updated_at <= s.order_updated_at;

-- 2. Insert staged lines that are not (or no longer) in the fact table.
--    Business keys are swapped for surrogate keys; the customer join is POINT-IN-TIME.
INSERT INTO dw.fact_order_line
    (order_item_id, order_id, customer_key, product_key, date_key,
     order_ts, status, quantity, unit_price, line_amount, order_updated_at)
SELECT s.order_item_id, s.order_id, c.customer_key, p.product_key,
       to_char(s.order_date, 'YYYYMMDD')::INT,
       s.order_ts, s.status, s.quantity, s.unit_price, s.line_amount, s.order_updated_at
FROM staging.order_lines AS s
JOIN dw.dim_customer AS c
  ON c.customer_id = s.customer_id
 AND s.order_ts >= c.valid_from
 AND s.order_ts <  c.valid_to
JOIN dw.dim_product AS p
  ON p.product_id = s.product_id
WHERE NOT EXISTS (
    SELECT 1 FROM dw.fact_order_line f WHERE f.order_item_id = s.order_item_id
);