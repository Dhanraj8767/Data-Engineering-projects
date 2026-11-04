# Databricks notebook source

# ==========================================
# RETAIL SALES - SILVER TRANSFORMATION
# ==========================================

from pyspark.sql import functions as F

# Step 1: Read data from Bronze Layer
bronze_df = spark.table("bronze_orders")

print("Bronze records:", bronze_df.count())

# Step 2: Remove duplicates and clean data
silver_df = (
    bronze_df
    .dropDuplicates(["order_id"])
    .filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("quantity").isNotNull() &
        F.col("price").isNotNull() &
        F.col("order_date").isNotNull() &
        (F.col("quantity") > 0) &
        (F.col("price") >= 0)
    )
    .withColumn("order_date", F.to_date("order_date"))
)

# Step 3: Save as Silver Delta Table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

print("Silver table created successfully!")

# Step 4: Validate Silver Data
print("Silver records:", silver_df.count())

display(spark.table("silver_orders"))
