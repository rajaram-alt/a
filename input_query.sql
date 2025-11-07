WITH cte_sales AS (
    SELECT s.id, s.amount, c.name
    FROM schema1.sales s
    JOIN customers c ON s.customer_id = c.id
),
cte_top_customers AS (
    SELECT name, SUM(amount) AS total
    FROM cte_sales
    GROUP BY name
)
SELECT *
FROM cte_top_customers;
