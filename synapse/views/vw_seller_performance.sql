-- ============================================================
-- Seller Performance Analysis
-- ============================================================
CREATE OR ALTER VIEW vw_seller_performance AS
SELECT
    g.state,
    g.city,
    g.region,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_payment_value) AS total_revenue,
    AVG(f.delivery_days) AS avg_delivery_days,
    SUM(CASE WHEN f.is_late_delivery = 1 THEN 1 ELSE 0 END) AS late_deliveries,
    CAST(SUM(CASE WHEN f.is_late_delivery = 1 THEN 1.0 ELSE 0 END) /
         NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,2)) AS late_delivery_pct,
    AVG(f.total_payment_value) AS avg_order_value,
    SUM(f.total_freight_value) AS total_freight_cost,
    CAST(SUM(f.total_freight_value) /
         NULLIF(SUM(f.total_payment_value), 0) * 100 AS DECIMAL(5,2)) AS freight_pct_of_revenue
FROM fact_orders f
    INNER JOIN dim_customers c ON f.customer_id = c.customer_id AND c.is_current = 1
    INNER JOIN dim_geography g ON c.customer_zip_code_prefix = g.zip_code_prefix
WHERE f.order_status = 'DELIVERED'
GROUP BY g.state, g.city, g.region;
GO
