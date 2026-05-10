-- ============================================================
-- Daily Revenue Dashboard
-- ============================================================
CREATE OR ALTER VIEW vw_daily_revenue AS
SELECT
    d.date,
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    d.year_quarter,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.order_id) AS daily_orders,
    SUM(f.total_payment_value) AS daily_revenue,
    SUM(f.total_freight_value) AS daily_freight_cost,
    AVG(f.total_payment_value) AS avg_order_value,
    SUM(f.item_count) AS items_sold,
    -- Running totals (use in Power BI DAX instead for performance)
    SUM(CASE WHEN f.order_status = 'CANCELED' THEN 1 ELSE 0 END) AS canceled_orders,
    CAST(SUM(CASE WHEN f.order_status = 'CANCELED' THEN 1.0 ELSE 0 END) /
         NULLIF(COUNT(*), 0) * 100 AS DECIMAL(5,2)) AS cancellation_rate
FROM fact_orders f
    INNER JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY
    d.date, d.year, d.month, d.month_name,
    d.year_month, d.year_quarter, d.day_name, d.is_weekend;
GO
