-- Revenue by city: what SCD Type 2 buys you
--   as_was = city the customer lived in WHEN they ordered
--   as_is  = city the customer lives in NOW
SELECT coalesce(w.city, i.city)                        AS city,
       coalesce(w.revenue, 0)                          AS revenue_as_was,
       coalesce(i.revenue, 0)                          AS revenue_as_is,
       coalesce(i.revenue, 0) - coalesce(w.revenue, 0) AS difference
FROM (
    SELECT c.city, sum(f.line_amount) AS revenue
    FROM dw.fact_order_line f
    JOIN dw.dim_customer c ON c.customer_key = f.customer_key
    WHERE f.status <> 'cancelled'
    GROUP BY c.city
) w
FULL JOIN (
    SELECT cur.city, sum(f.line_amount) AS revenue
    FROM dw.fact_order_line f
    JOIN dw.dim_customer c   ON c.customer_key = f.customer_key
    JOIN dw.dim_customer cur ON cur.customer_id = c.customer_id AND cur.is_current
    WHERE f.status <> 'cancelled'
    GROUP BY cur.city
) i ON i.city = w.city
ORDER BY revenue_as_was DESC;