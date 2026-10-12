-- SCD Type 2 in two statements.

-- Step 1: EXPIRE the current version when a tracked attribute changed.
--   * IS DISTINCT FROM          -> only real changes create history (re-running a day is a no-op)
--   * s.updated_at > d.valid_from -> an old batch (backfill) can never expire a newer version
UPDATE dw.dim_customer AS d
SET valid_to   = s.updated_at,
    is_current = FALSE
FROM staging.customers AS s
WHERE d.customer_id = s.customer_id
  AND d.is_current
  AND s.updated_at > d.valid_from
  AND (d.name, d.email, d.city, d.segment) IS DISTINCT FROM (s.name, s.email, s.city, s.segment);

-- Step 2: INSERT a new current version for every staged customer that has none:
--   brand-new customers, plus the ones step 1 just expired.
--   A customer's very first version is valid from 1900-01-01 so every fact can find a version.
INSERT INTO dw.dim_customer (customer_id, name, email, city, segment, valid_from, valid_to, is_current)
SELECT s.customer_id, s.name, s.email, s.city, s.segment,
       CASE WHEN EXISTS (SELECT 1 FROM dw.dim_customer h WHERE h.customer_id = s.customer_id)
            THEN s.updated_at
            ELSE TIMESTAMP '1900-01-01'
       END,
       TIMESTAMP '9999-12-31',
       TRUE
FROM staging.customers AS s
WHERE NOT EXISTS (
    SELECT 1 FROM dw.dim_customer c
    WHERE c.customer_id = s.customer_id AND c.is_current
);