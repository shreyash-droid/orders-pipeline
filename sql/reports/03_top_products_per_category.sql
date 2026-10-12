-- Top 3 products per category by revenue (window function)
SELECT category, product, units, revenue, rank_in_category
FROM (
    SELECT p.category,
           p.name             AS product,
           sum(f.quantity)    AS units,
           sum(f.line_amount) AS revenue,
           rank() OVER (PARTITION BY p.category ORDER BY sum(f.line_amount) DESC) AS rank_in_category
    FROM dw.fact_order_line f
    JOIN dw.dim_product p ON p.product_key = f.product_key
    WHERE f.status <> 'cancelled'
    GROUP BY p.category, p.name
) ranked
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;