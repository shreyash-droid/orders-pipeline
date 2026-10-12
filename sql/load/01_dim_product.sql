-- SCD Type 1 upsert: overwrite the row, keep no history.
-- The WHERE guard stops an old batch (a backfill) from overwriting newer data.
INSERT INTO dw.dim_product (product_id, name, category, price, updated_at)
SELECT product_id, name, category, price, updated_at
FROM staging.products
ON CONFLICT (product_id) DO UPDATE
SET name       = EXCLUDED.name,
    category   = EXCLUDED.category,
    price      = EXCLUDED.price,
    updated_at = EXCLUDED.updated_at
WHERE dw.dim_product.updated_at < EXCLUDED.updated_at;