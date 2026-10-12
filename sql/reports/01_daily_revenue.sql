-- Daily revenue (cancelled orders excluded)
SELECT d.full_date                AS day,
       count(DISTINCT f.order_id) AS orders,
       sum(f.quantity)            AS units,
       sum(f.line_amount)         AS revenue
FROM dw.fact_order_line f
JOIN dw.dim_date d ON d.date_key = f.date_key
WHERE f.status <> 'cancelled'
GROUP BY d.full_date
ORDER BY d.full_date;