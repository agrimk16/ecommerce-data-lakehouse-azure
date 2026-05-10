-- ============================================================
-- Stored Procedure: Refresh Materialized Views / Cache
-- ============================================================
-- Example of using stored procedures in Synapse for
-- scheduled cache refresh or data quality checks.
-- ============================================================

CREATE OR ALTER PROCEDURE sp_refresh_materialized_views
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2 = GETUTCDATE();
    DECLARE @row_count INT;

    PRINT '=== Starting Data Quality & Refresh Process ===';
    PRINT 'Start time: ' + CAST(@start_time AS VARCHAR(50));

    -- Data Quality Check 1: Orphan orders (no matching customer)
    SELECT @row_count = COUNT(*)
    FROM fact_orders f
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id AND c.is_current = 1
    WHERE c.customer_id IS NULL;

    PRINT 'Orphan orders (no customer match): ' + CAST(@row_count AS VARCHAR(20));

    -- Data Quality Check 2: Orders with zero payment
    SELECT @row_count = COUNT(*)
    FROM fact_orders
    WHERE total_payment_value IS NULL OR total_payment_value = 0;

    PRINT 'Orders with zero/null payment: ' + CAST(@row_count AS VARCHAR(20));

    -- Data Quality Check 3: Future-dated orders
    SELECT @row_count = COUNT(*)
    FROM fact_orders
    WHERE order_purchase_timestamp > GETUTCDATE();

    PRINT 'Future-dated orders: ' + CAST(@row_count AS VARCHAR(20));

    -- Summary stats
    PRINT '';
    PRINT '=== Summary Statistics ===';

    SELECT
        'Fact Orders' AS table_name,
        COUNT(*) AS row_count,
        MIN(order_purchase_timestamp) AS min_date,
        MAX(order_purchase_timestamp) AS max_date
    FROM fact_orders;

    PRINT '';
    PRINT 'Completed in ' +
        CAST(DATEDIFF(SECOND, @start_time, GETUTCDATE()) AS VARCHAR(10)) + ' seconds';
    PRINT '=== Process Complete ===';
END;
GO
