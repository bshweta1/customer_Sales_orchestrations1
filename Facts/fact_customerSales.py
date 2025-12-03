# fact_sales.py
import sys
from pyspark.sql.functions import col, sum as _sum, count as _count
from env_setup import get_spark, get_paths

# ------------------------------
# --- 0. SPARK SESSION & PATHS ---
# ------------------------------
spark = get_spark(app_name="fact_cutomersales_etl")
bucket = "customerorderstoredata"

# Optional: set AWS credentials (if not already set in environment)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "YOUR_SECRET_KEY")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

bucket = "customerorderstoredata"

# ------------------------------
# --- 1. DEFINE PATHS ---
# ------------------------------
fact_customerSales_path = f"s3a://{bucket}/Transform_data/fact/fact_CustomerSales"
dim_customer_path = f"s3a://{bucket}/Transform_data/Dimension/dim_customers"
dim_product_path = f"s3a://{bucket}/Transform_data/Dimension/dim_products"
dim_store_path = f"s3a://{bucket}/Transform_data/Dimension/dim_stores"
dim_order_path = f"s3a://{bucket}/Transform_data/Dimension/dim_orders"
dim_order_items_path = f"s3a://{bucket}/Transform_data/Dimension/dim_order_items"

# ------------------------------
# --- 2. LOAD DIMENSIONS ---
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
# --- 3. BUILD FACT CUSTOMER SALES ---
# ------------------------------
fact_customerSales = (
    dim_order_items.alias("oi")
    .join(dim_orders.alias("o"), "ORDER_ID", "inner")
    .join(dim_customers.alias("c"), "CUSTOMER_ID", "left")
    .join(dim_products.alias("p"), "PRODUCT_ID", "left")
    .join(dim_stores.alias("s"), "STORE_ID", "left")
    .select(
        col("oi.ORDER_ID"),
        col("oi.LINE_ITEM_ID").alias("ORDER_ITEM_ID"),  # use LINE_ITEM_ID and alias it
        col("o.ORDER_DATETIME").alias("ORDER_DATE"),   # use ORDER_DATETIME and alias it
        col("c.CUSTOMER_ID"),
        col("p.PRODUCT_ID"),
        col("s.STORE_ID"),
        col("oi.QUANTITY"),
        col("oi.UNIT_PRICE"),
        (col("oi.QUANTITY") * col("oi.UNIT_PRICE")).alias("TOTAL_AMOUNT")
    )
)


# ------------------------------
# --- 4. WRITE FACT TABLE ---
# ------------------------------
fact_customerSales.write.format("delta").mode("overwrite").save(fact_customerSales_path)


# ------------------------------
# --- 5. VALIDATION ---
# ------------------------------
print("Fact Customer Sales preview:")
fact_customerSales.show(10, truncate=False)

spark.stop()
print("Fact Customer Sales ETL completed successfully.")