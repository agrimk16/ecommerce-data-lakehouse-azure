"""
Data Quality Tests — Silver Layer

Validates cleansed data in the Silver Delta Lake tables.
"""

from pyspark.sql.functions import col


def test_silver_orders_no_nulls():
    """Primary keys should never be null."""
    df = spark.read.format("delta").load("/mnt/silver/orders")
    null_count = df.filter(col("order_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} null order_ids in Silver!"
    print(f"  ✓ Silver orders: No null primary keys")


def test_silver_orders_no_duplicates():
    """No duplicate order_ids."""
    df = spark.read.format("delta").load("/mnt/silver/orders")
    total = df.count()
    distinct = df.select("order_id").distinct().count()
    assert total == distinct, f"Duplicates found! Total: {total:,}, Distinct: {distinct:,}"
    print(f"  ✓ Silver orders: No duplicates ({total:,} records)")


def test_silver_orders_valid_status():
    """All order statuses should be in valid set."""
    df = spark.read.format("delta").load("/mnt/silver/orders")
    valid_statuses = {"DELIVERED", "SHIPPED", "PROCESSING", "CANCELED",
                      "UNAVAILABLE", "INVOICED", "CREATED", "APPROVED"}
    actual_statuses = set(row.order_status for row in df.select("order_status").distinct().collect())
    invalid = actual_statuses - valid_statuses
    assert len(invalid) == 0, f"Invalid statuses found: {invalid}"
    print(f"  ✓ Silver orders: All statuses valid ({len(actual_statuses)} types)")


def test_silver_customers_no_nulls():
    """Customer primary key not null."""
    df = spark.read.format("delta").load("/mnt/silver/customers")
    null_count = df.filter(col("customer_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} null customer_ids!"
    print(f"  ✓ Silver customers: No null primary keys")


def test_silver_payments_positive():
    """All payment values should be non-negative."""
    df = spark.read.format("delta").load("/mnt/silver/payment_summary")
    negative = df.filter(col("total_payment_value") < 0).count()
    assert negative == 0, f"Found {negative} negative payment values!"
    print(f"  ✓ Silver payment_summary: All values non-negative")


def test_silver_metadata_columns():
    """All Silver tables should have audit metadata columns."""
    tables = ["orders", "customers", "products"]
    expected_cols = ["ingestion_timestamp", "source_system", "record_hash"]

    for table in tables:
        df = spark.read.format("delta").load(f"/mnt/silver/{table}")
        for col_name in expected_cols:
            assert col_name in df.columns, f"Missing {col_name} in Silver/{table}"
    print(f"  ✓ All Silver tables have metadata columns")


if __name__ == "__main__" or True:
    print("=" * 50)
    print("SILVER LAYER DATA QUALITY TESTS")
    print("=" * 50)

    tests = [
        test_silver_orders_no_nulls,
        test_silver_orders_no_duplicates,
        test_silver_orders_valid_status,
        test_silver_customers_no_nulls,
        test_silver_payments_positive,
        test_silver_metadata_columns,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except (AssertionError, Exception) as e:
            print(f"  ✗ FAILED: {test.__name__}: {e}")
            failed += 1

    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
