-- ============================================================
-- Sales by Product Category — Top revenue categories
-- ============================================================
-- NOTE: fact_orders is at order grain (one row per order).
-- We join via dim_products using the most common product in the order.
-- For a full product-level breakdown, a fact_order_items table would be needed.
-- This view groups by the product dimension attributes that best describe
-- the order (approximated via a bridge through order_items in Synapse).
-- ============================================================
CREATE OR ALTER VIEW vw_sales_by_category AS
SELECT
    p.category,
    p.size_category,
    COUNT(DISTINCT f.order_id)         AS order_count,
    SUM(f.total_item_value)            AS total_revenue,
    AVG(f.total_item_value)            AS avg_order_value,
    SUM(f.item_count)                  AS total_items_sold,
    AVG(f.delivery_days)               AS avg_delivery_days,
    SUM(CASE WHEN f.is_late_delivery = 1 THEN 1 ELSE 0 END) AS late_deliveries,
    CAST(
        SUM(CASE WHEN f.is_late_delivery = 1 THEN 1.0 ELSE 0 END) /
        NULLIF(COUNT(*), 0) * 100
    AS DECIMAL(5,2))                   AS late_delivery_pct
FROM (
    -- Subquery: pick the first product_id per order from the Silver order_items table
    SELECT
        oi.order_id,
        FIRST_VALUE(oi.product_id) OVER (
            PARTITION BY oi.order_id
            ORDER BY oi.order_item_id
        ) AS primary_product_id
    FROM OPENROWSET(
        BULK 'order_items/',
        DATA_SOURCE = 'SilverDataLake',
        FORMAT = 'DELTA'
    ) AS oi
) AS order_product
INNER JOIN fact_orders f    ON order_product.order_id       = f.order_id
INNER JOIN dim_products p   ON order_product.primary_product_id = p.product_id
INNER JOIN dim_date d       ON f.order_date_key              = d.date_key
WHERE f.order_status = 'DELIVERED'
GROUP BY p.category, p.size_category;
GO
