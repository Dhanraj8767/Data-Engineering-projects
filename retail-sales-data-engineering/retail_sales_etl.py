
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# 1. Create sample retail sales data
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("region", StringType(), True)
])

data = [
    ("ORD001", "2026-01-05", "Laptop", "Electronics", 2, 55000.0, "West"),
    ("ORD002", "2026-01-08", "Mouse", "Accessories", 5, 500.0, "South"),
    ("ORD003", "2026-01-12", "Chair", "Furniture", 3, 3500.0, "North"),
    ("ORD004", "2026-02-02", "Keyboard", "Accessories", 4, 1200.0, "West"),
    ("ORD005", "2026-02-10", "Monitor", "Electronics", 2, 12000.0, "East"),
    ("ORD006", "2026-02-15", "Desk", "Furniture", 1, 8000.0, "South"),
    ("ORD007", "2026-03-01", "Laptop", "Electronics", 1, 55000.0, "North"),
    ("ORD008", "2026-03-06", "Mouse", "Accessories", 10, 500.0, "East"),
    ("ORD009", "2026-03-11", "Chair", "Furniture", 2, 3500.0, "West"),
    ("ORD010", "2026-03-18", "Monitor", "Electronics", 3, 12000.0, "South")
]

raw_df = spark.createDataFrame(data, schema)

# 2. Bronze layer: preserve raw data
bronze_df = raw_df.withColumn("ingestion_timestamp", F.current_timestamp())

bronze_path = "/tmp/retail_sales/bronze"
bronze_df.write.format("delta").mode("overwrite").save(bronze_path)

# 3. Silver layer: clean and validate
silver_df = (
    bronze_df
    .dropDuplicates(["order_id"])
    .filter(
        F.col("order_id").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0)
    )
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn(
        "sales_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
)

silver_path = "/tmp/retail_sales/silver"
silver_df.write.format("delta").mode("overwrite").save(silver_path)

# 4. Gold layer: sales summaries
gold_df = (
    silver_df
    .groupBy("category", "region")
    .agg(
        F.sum("sales_amount").alias("total_sales"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("order_id").alias("total_orders")
    )
    .orderBy("category", "region")
)

gold_path = "/tmp/retail_sales/gold"
gold_df.write.format("delta").mode("overwrite").save(gold_path)

# 5. Display results
print("BRONZE LAYER")
display(spark.read.format("delta").load(bronze_path))

print("SILVER LAYER")
display(spark.read.format("delta").load(silver_path))

print("GOLD LAYER")
display(spark.read.format("delta").load(gold_path))

print("ETL pipeline completed successfully.")
