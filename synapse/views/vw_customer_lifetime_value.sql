-- ============================================================
-- Customer Lifetime Value (CLV)
-- ============================================================
CREATE OR ALTER VIEW vw_customer_lifetime_value AS
SELECT
    c.customer_unique_id,
    c.customer_city AS city,
    c.customer_state AS state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_payment_value) AS lifetime_value,
    AVG(f.total_payment_value) AS avg_order_value,
    MIN(f.order_purchase_timestamp) AS first_order_date,
    MAX(f.order_purchase_timestamp) AS last_order_date,
    DATEDIFF(DAY,
        MIN(f.order_purchase_timestamp),
        MAX(f.order_purchase_timestamp)
    ) AS customer_tenure_days,
    AVG(f.delivery_days) AS avg_delivery_days,
    -- Segment by value
    CASE
        WHEN SUM(f.total_payment_value) > 500 THEN 'High Value'
        WHEN SUM(f.total_payment_value) > 200 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_segment
FROM fact_orders f
    INNER JOIN dim_customers c ON f.customer_id = c.customer_id AND c.is_current = 1
WHERE f.order_status IN ('DELIVERED', 'SHIPPED')
GROUP BY c.customer_unique_id, c.customer_city, c.customer_state;
GO
