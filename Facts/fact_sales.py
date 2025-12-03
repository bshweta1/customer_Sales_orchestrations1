# fact_sales.py
import sys
from pyspark.sql.functions import col, sum as _sum, count as _count
from env_setup import get_spark, get_paths

# ------------------------------
# --- 0. SPARK SESSION & PATHS ---
# ------------------------------
spark = get_spark(app_name="fact_sales_etl")
bucket = "customerorderstoredata"

fact_sales_path = f"s3a://{bucket}/Transform_data/fact/fact_sales"
dim_customer_path = f"s3a://{bucket}/Transform_data/Dimension/dim_customers"
dim_product_path = f"s3a://{bucket}/Transform_data/Dimension/dim_products"
dim_store_path = f"s3a://{bucket}/Transform_data/Dimension/dim_stores"
dim_order_path = f"s3a://{bucket}/Transform_data/Dimension/dim_orders"
dim_order_items_path = f"s3a://{bucket}/Transform_data/Dimension/dim_order_items"



# ------------------------------
# --- 1. LOAD DIMENSIONS ---
# ------------------------------
try:
    dim_customers = spark.read.format("delta").load(dim_customer_path)
    dim_products = spark.read.format("delta").load(dim_product_path)
    dim_stores = spark.read.format("delta").load(dim_store_path)
    dim_orders = spark.read.format("delta").load(dim_order_path)
    dim_order_items = spark.read.format("delta").load(dim_order_items_path)
except Exception as e:
    print(f"Error loading dimension tables: {e}")
    spark.stop()
    sys.exit(1)

# ------------------------------
# --- 2. BUILD FACT SALES ---
# ------------------------------
# Join fact tables with dimensions
fact_sales = (
    dim_order_items.alias("oi")
    .join(dim_orders.alias("o"), "ORDER_ID", "inner")
    .join(dim_customers.alias("c"), "CUSTOMER_ID", "left")
    .join(dim_products.alias("p"), "PRODUCT_ID", "left")
    .join(dim_stores.alias("s"), "STORE_ID", "left")
    .withColumn("TOTAL_AMOUNT", col("oi.QUANTITY") * col("oi.UNIT_PRICE"))
)


# Optional: Aggregate metrics at different levels
sales_summary = (
    fact_sales.groupBy("STORE_ID", "PRODUCT_ID")
    .agg(
        _sum("TOTAL_AMOUNT").alias("TOTAL_SALES_AMOUNT"),
        _sum("QUANTITY").alias("TOTAL_QUANTITY_SOLD"),
        _count("ORDER_ID").alias("NUM_ORDERS")
    )
)

# ------------------------------
# --- 3. WRITE FACT TABLE ---
# ------------------------------
# fact_sales_path = "s3a://customerorderstoredata/fact/fact_sales"
# update your S3 path
sales_summary.write.format("delta").mode("overwrite").save(fact_sales_path)

# ------------------------------
# --- 4. VALIDATION ---
# ------------------------------
print("Fact table preview:")
sales_summary.show(10, truncate=False)

spark.stop()
print("Fact ETL completed successfully.")
