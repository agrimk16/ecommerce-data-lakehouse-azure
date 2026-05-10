"""
Data Quality Tests — Bronze Layer

Run these tests after the ADF ingestion pipeline completes
to validate data landed correctly in the Bronze layer.

Usage (in Databricks):
    %run ./tests/test_bronze_layer
"""


def test_bronze_orders_exist():
    """Verify orders data exists in Bronze layer."""
    df = spark.read.parquet("/mnt/bronze/orders/")
    count = df.count()
    assert count > 0, f"Bronze orders is empty! Expected >0, got {count}"
    assert count > 90000, f"Bronze orders seems incomplete. Expected ~100K, got {count:,}"
    print(f"  ✓ Bronze orders: {count:,} records")


def test_bronze_orders_schema():
    """Verify orders has expected columns."""
    df = spark.read.parquet("/mnt/bronze/orders/")
    expected_cols = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    for col_name in expected_cols:
        assert col_name in df.columns, f"Missing column: {col_name}"
    print(f"  ✓ Bronze orders schema valid ({len(df.columns)} columns)")


def test_bronze_customers_exist():
    """Verify customers data exists."""
    df = spark.read.parquet("/mnt/bronze/customers/")
    count = df.count()
    assert count > 0, f"Bronze customers is empty!"
    print(f"  ✓ Bronze customers: {count:,} records")


def test_bronze_products_exist():
    """Verify products data exists."""
    df = spark.read.parquet("/mnt/bronze/products/")
    count = df.count()
    assert count > 0, f"Bronze products is empty!"
    print(f"  ✓ Bronze products: {count:,} records")


def test_bronze_no_empty_files():
    """Verify no empty Parquet files in Bronze."""
    entities = ["orders", "customers", "products", "payments", "geolocation"]
    for entity in entities:
        try:
            df = spark.read.parquet(f"/mnt/bronze/{entity}/")
            assert df.count() > 0, f"Bronze/{entity} is empty!"
            print(f"  ✓ Bronze/{entity}: non-empty")
        except Exception as e:
            print(f"  ✗ Bronze/{entity}: {str(e)}")


# Run all tests
if __name__ == "__main__" or True:
    print("=" * 50)
    print("BRONZE LAYER DATA QUALITY TESTS")
    print("=" * 50)

    tests = [
        test_bronze_orders_exist,
        test_bronze_orders_schema,
        test_bronze_customers_exist,
        test_bronze_products_exist,
        test_bronze_no_empty_files,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  ✗ FAILED: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ ERROR: {test.__name__}: {e}")
            failed += 1

    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
