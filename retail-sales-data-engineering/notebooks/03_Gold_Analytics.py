# Databricks notebook source

# =====================================
# RETAIL SALES - GOLD ANALYTICS
# =====================================

from pyspark.sql import functions as F

# Step 1: Read Silver Data
silver_df = spark.table("silver_orders")

# Step 2: Calculate Total Revenue
gold_sales_df = silver_df.withColumn(
    "total_amount",
    F.col("quantity") * F.col("price")
)

# Step 3: Save Gold Delta Table
gold_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales")

print("Gold table created successfully!")

# Step 4: Overall Sales KPIs
spark.sql("""
SELECT
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue
FROM gold_sales
""").show()

# Step 5: Daily Revenue
spark.sql("""
SELECT
    order_date,
    SUM(total_amount) AS daily_revenue
FROM gold_sales
GROUP BY order_date
ORDER BY order_date
""").show()

# Step 6: Product-wise Revenue
spark.sql("""
SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue
FROM gold_sales
GROUP BY product_id
ORDER BY total_revenue DESC
""").show()

# Step 7: Data Quality Validation
gold_df = spark.table("gold_sales")

assert gold_df.count() == 8
assert gold_df.filter("order_id IS NULL").count() == 0
assert gold_df.filter("total_amount < 0").count() == 0

print("All Gold data quality checks passed!")
