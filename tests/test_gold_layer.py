"""
Data Quality Tests — Gold Layer (Star Schema)

Validates the star schema tables in the Gold layer.
"""

from pyspark.sql.functions import col, count as spark_count


def test_gold_fact_orders_exists():
    """Fact orders table exists and has data."""
    df = spark.read.format("delta").load("/mnt/gold/fact_orders")
    assert df.count() > 0, "fact_orders is empty!"
    print(f"  ✓ Gold fact_orders: {df.count():,} records")


def test_gold_fact_orders_measures():
    """Fact table should have numeric measures."""
    df = spark.read.format("delta").load("/mnt/gold/fact_orders")
    measures = ["total_item_value", "total_freight_value", "total_payment_value",
                "item_count", "delivery_days"]
    for m in measures:
        assert m in df.columns, f"Missing measure: {m}"
    print(f"  ✓ Gold fact_orders: All measures present")


def test_gold_dim_customers_scd():
    """dim_customers should have SCD Type 2 columns."""
    df = spark.read.format("delta").load("/mnt/gold/dim_customers")
    scd_cols = ["customer_sk", "is_current", "valid_from", "valid_to"]
    for c in scd_cols:
        assert c in df.columns, f"Missing SCD column: {c}"
    # Current records should exist
    current = df.filter(col("is_current") == True).count()
    assert current > 0, "No current customer records!"
    print(f"  ✓ Gold dim_customers SCD: {current:,} current records")


def test_gold_dim_date_complete():
    """dim_date should cover the full date range."""
    df = spark.read.format("delta").load("/mnt/gold/dim_date")
    count = df.count()
    # Should have at least 10 years * 365 days
    assert count > 3650, f"dim_date seems incomplete: {count} days"
    print(f"  ✓ Gold dim_date: {count:,} days")


def test_gold_referential_integrity():
    """Check fact→dimension referential integrity."""
    df_fact = spark.read.format("delta").load("/mnt/gold/fact_orders")
    df_dim_date = spark.read.format("delta").load("/mnt/gold/dim_date")

    # All date keys in fact should exist in dim_date
    orphan_dates = (
        df_fact.select("order_date_key").distinct()
        .join(df_dim_date.select("date_key"), df_fact.order_date_key == df_dim_date.date_key, "left_anti")
        .count()
    )
    assert orphan_dates == 0, f"Found {orphan_dates} orphan date keys!"
    print(f"  ✓ Gold referential integrity: fact→dim_date OK")


if __name__ == "__main__" or True:
    print("=" * 50)
    print("GOLD LAYER DATA QUALITY TESTS")
    print("=" * 50)

    tests = [
        test_gold_fact_orders_exists,
        test_gold_fact_orders_measures,
        test_gold_dim_customers_scd,
        test_gold_dim_date_complete,
        test_gold_referential_integrity,
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
