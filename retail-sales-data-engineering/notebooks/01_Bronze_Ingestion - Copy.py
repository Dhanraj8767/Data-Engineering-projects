# Databricks notebook source
print("RETAIL SALES DATA ENGINEERING PROJECT")

# COMMAND ----------

print("Retail Sales Data Engineering Project")
print("Step 1: Databricks setup successful!")

# COMMAND ----------

orders_data = [
    (1001, "C01", "P01", 2, 500, "2026-10-01"),
    (1002, "C02", "P02", 1, 800, "2026-10-02"),
    (1003, "C03", "P03", 3, 200, "2026-10-02"),
    (1004, "C01", "P02", 2, 800, "2026-10-03"),
    (1005, "C04", "P01", 1, 500, "2026-10-03"),
    (1006, "C02", "P03", 4, 200, "2026-10-04"),
    (1007, "C05", "P04", 2, 1200, "2026-10-04"),
    (1008, "C03", "P01", 3, 500, "2026-10-05")
]

orders_columns = [
    "order_id", "customer_id", "product_id",
    "quantity", "price", "order_date"
]

orders_df = spark.createDataFrame(
    orders_data, orders_columns
)

display(orders_df)

# COMMAND ----------


# Step 3: Save raw orders into Bronze layer

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")

print("Bronze table created successfully!")

# COMMAND ----------


display(spark.table("bronze_orders"))

# COMMAND ----------


from pyspark.sql import functions as F

bronze_df = spark.table("bronze_orders")

print("Total Bronze records:", bronze_df.count())

print("Duplicate order IDs:",
      bronze_df.count() -
      bronze_df.select("order_id").distinct().count())

print("Rows with null values:")

bronze_df.filter(
    F.col("order_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("product_id").isNull() |
    F.col("quantity").isNull() |
    F.col("price").isNull() |
    F.col("order_date").isNull()
).show()

# COMMAND ----------


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

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

print("Silver table created successfully!")
print("Silver record count:", silver_df.count())

# COMMAND ----------


display(spark.table("silver_orders"))

# COMMAND ----------


from pyspark.sql import functions as F

silver_df = spark.table("silver_orders")

gold_sales_df = silver_df.withColumn(
    "total_amount",
    F.col("quantity") * F.col("price")
)

display(gold_sales_df)

# COMMAND ----------


gold_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales")

print("Gold sales table created successfully!")

# COMMAND ----------


from pyspark.sql import functions as F

# Read clean data from Silver layer
silver_df = spark.table("silver_orders")

# Calculate revenue for each order
gold_sales_df = silver_df.withColumn(
    "total_amount",
    F.col("quantity") * F.col("price")
)

# Save as Gold Delta table
gold_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales")

print("Gold sales table created successfully!")


# COMMAND ----------


spark.sql("""
    SELECT
        COUNT(*) AS total_orders,
        SUM(quantity) AS total_quantity,
        SUM(total_amount) AS total_revenue
    FROM gold_sales
""").show()


# COMMAND ----------


spark.sql("""
    SELECT
        order_date,
        SUM(total_amount) AS daily_revenue
    FROM gold_sales
    GROUP BY order_date
    ORDER BY order_date
""").show()


# COMMAND ----------


spark.sql("""
    SELECT
        product_id,
        SUM(quantity) AS total_quantity,
        SUM(total_amount) AS total_revenue
    FROM gold_sales
    GROUP BY product_id
    ORDER BY total_revenue DESC
""").show()


# COMMAND ----------


# Validate Gold table

gold_df = spark.table("gold_sales")

total_records = gold_df.count()
null_orders = gold_df.filter(
    "order_id IS NULL"
).count()
invalid_amounts = gold_df.filter(
    "total_amount < 0"
).count()

print("Total records:", total_records)
print("Null order IDs:", null_orders)
print("Invalid revenue records:", invalid_amounts)

assert total_records == 8, "Unexpected record count"
assert null_orders == 0, "Null order IDs found"
assert invalid_amounts == 0, "Negative revenue found"

print("All data quality checks passed!")
