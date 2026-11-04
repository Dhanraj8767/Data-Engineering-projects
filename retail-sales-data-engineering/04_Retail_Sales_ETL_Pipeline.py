from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Retail Sales ETL Pipeline") \
    .getOrCreate()

# Define paths
raw_path = "data/Retail_Sales_ETL_Pipeline.csv"
bronze_path = "/tmp/retail_sales/bronze"
silver_path = "/tmp/retail_sales/silver"
gold_path = "/tmp/retail_sales/gold"

# BRONZE: Extract and Load
df = spark.read.option("header", True).option(
    "inferSchema", True
).csv(raw_path)

df.write.format("delta").mode("overwrite").save(bronze_path)

# SILVER: Transform
bronze_df = spark.read.format("delta").load(bronze_path)

silver_df = bronze_df.dropDuplicates().dropna(
    subset=["order_id"]
)

silver_df.write.format("delta").mode("overwrite").save(silver_path)

gold_df = silver_df.groupBy("product").agg(
    F.sum("quantity").alias("total_quantity"),
    F.sum(
        F.col("quantity") * F.col("unit_price")
    ).alias("total_sales")
)

gold_df.write.format("delta").mode("overwrite").save(gold_path)

print("Retail Sales ETL Pipeline Completed Successfully!")
