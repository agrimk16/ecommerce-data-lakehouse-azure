# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: Dimension — Date
# MAGIC
# MAGIC Creates `dim_date` — a standard date dimension covering the full date range of our data.
# MAGIC
# MAGIC **Skills you'll learn:**
# MAGIC - Generating date ranges in PySpark
# MAGIC - Building a date dimension with fiscal periods, holidays, etc.
# MAGIC - Why date dimensions matter (vs just using raw dates)

# COMMAND ----------

from pyspark.sql.functions import (
    col, explode, sequence, to_date, lit, date_format,
    year, month, dayofmonth, dayofweek, weekofyear, quarter,
    when, last_day, datediff
)
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Date Range

# COMMAND ----------

# Generate dates from 2016-01-01 to 2026-12-31 (covers Olist data + future)
df_dates = (
    spark.range(1)
    .select(
        explode(
            sequence(
                to_date(lit("2016-01-01")),
                to_date(lit("2026-12-31")),
                expr("interval 1 day")
            )
        ).alias("date")
    )
)

# COMMAND ----------

from pyspark.sql.functions import expr

# Build full date dimension
df_dim_date = (
    df_dates
    .select(
        # Date key (integer): 20260115
        date_format(col("date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("date"),
        # Year
        year(col("date")).alias("year"),
        # Quarter
        quarter(col("date")).alias("quarter"),
        # Month
        month(col("date")).alias("month"),
        date_format(col("date"), "MMMM").alias("month_name"),
        date_format(col("date"), "MMM").alias("month_short"),
        # Week
        weekofyear(col("date")).alias("week_of_year"),
        # Day
        dayofmonth(col("date")).alias("day_of_month"),
        dayofweek(col("date")).alias("day_of_week"),  # 1=Sun, 7=Sat
        date_format(col("date"), "EEEE").alias("day_name"),
        date_format(col("date"), "EEE").alias("day_short"),
        # Flags
        when(dayofweek(col("date")).isin(1, 7), True).otherwise(False).alias("is_weekend"),
        # Year-Month key for aggregation
        date_format(col("date"), "yyyy-MM").alias("year_month"),
        # Year-Quarter key
        expr("concat(year(date), '-Q', quarter(date))").alias("year_quarter"),
        # Day of year
        date_format(col("date"), "D").cast("int").alias("day_of_year"),
        # Is month end
        when(col("date") == last_day(col("date")), True).otherwise(False).alias("is_month_end"),
        # Is month start
        when(dayofmonth(col("date")) == 1, True).otherwise(False).alias("is_month_start"),
    )
)

print(f"dim_date: {df_dim_date.count():,} records")
display(df_dim_date.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold

# COMMAND ----------

(
    df_dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/gold/dim_date")
)

print("✓ dim_date written to Gold layer")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/gold/dim_date';

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What You Learned
# MAGIC
# MAGIC 1. **Date dimension design** — Standard DW pattern for time-based analysis
# MAGIC 2. **`sequence()` + `explode()`** — Generating date ranges in PySpark
# MAGIC 3. **Date functions** — `year()`, `quarter()`, `dayofweek()`, `date_format()`
# MAGIC 4. **Date keys** — Integer keys (20260115) for fast joins with fact tables
# MAGIC 5. **Why dim_date** — Enables "drill-down" from Year → Quarter → Month → Day
