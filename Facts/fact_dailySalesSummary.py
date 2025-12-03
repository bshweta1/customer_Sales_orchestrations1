import sys
from pyspark.sql.functions import sum as _sum, count, col

from Facts.fact_sales import fact_sales
from env_setup import get_spark
from Facts.fact_customerSales import fact_customerSales_path  # assuming you have the path

# ------------------------------
# --- 0. SPARK SESSION & PATHS ---
# ------------------------------
spark = get_spark("fact_dailySalesSummary")
bucket = "customerorderstoredata"

# # Load the fact_customerSales table
fact_Sales = spark.read.format("delta").load(f"s3a://{bucket}/Transform_data/fact/fact_sales")
dim_orders = spark.read.format("delta").load(f"s3a://{bucket}/Transform_data/Dimension/dim_orders")
# ------------------------------
# --- 1. BUILD DAILY SALES SUMMARY ---
# ------------------------------
daily_summary = (
    fact_sales.alias("fs")
    .join(dim_orders.alias("o"), "ORDER_ID", "left")
    .groupBy(
        col("o.ORDER_DATETIME").alias("ORDER_DATE"),  # ensure correct column
        col("fs.STORE_ID"),
        col("fs.PRODUCT_ID")
    )
    .agg(
        _sum("fs.QUANTITY").alias("TOTAL_QTY"),
        _sum("fs.TOTAL_AMOUNT").alias("TOTAL_REVENUE"),
        count("fs.ORDER_ID").alias("TOTAL_ORDERS")
    )
)
# ------------------------------
# --- 2. WRITE FACT DAILY SALES SUMMARY ---
# ------------------------------
daily_summary_path = f"s3a://{bucket}/Transform_data/fact/fact_DailySalesSummary"
daily_summary.write.format("delta").mode("overwrite").save(daily_summary_path)

print("fact_daily_sales_summary created.")
