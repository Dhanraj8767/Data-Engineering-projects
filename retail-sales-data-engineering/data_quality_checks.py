
from pyspark.sql import functions as F

# Read the Silver layer
silver_path = "/tmp/retail_sales/silver"
silver_df = spark.read.format("delta").load(silver_path)

# 1. Total records
total_records = silver_df.count()

# 2. Check null values in important columns
required_columns = [
    "order_id",
    "order_date",
    "product",
    "quantity",
    "unit_price"
]

print("DATA QUALITY REPORT")
print("-------------------")
print(f"Total records: {total_records}")

for column_name in required_columns:
    null_count = silver_df.filter(
        F.col(column_name).isNull()
    ).count()

    print(f"{column_name} null count: {null_count}")

# 3. Check duplicate order IDs
duplicate_count = (
    silver_df.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicate order IDs: {duplicate_count}")

# 4. Check invalid quantity and price
invalid_quantity = silver_df.filter(
    F.col("quantity") <= 0
).count()

invalid_price = silver_df.filter(
    F.col("unit_price") < 0
).count()

print(f"Invalid quantity records: {invalid_quantity}")
print(f"Invalid price records: {invalid_price}")

# 5. Overall quality status
if (
    sum(
        silver_df.filter(F.col(c).isNull()).count()
        for c in required_columns
    ) == 0
    and duplicate_count == 0
    and invalid_quantity == 0
    and invalid_price == 0
):
    print("Quality status: PASS")
else:
    print("Quality status: REVIEW REQUIRED")
