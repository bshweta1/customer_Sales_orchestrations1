import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id
from delta.tables import DeltaTable

# ------------------------------
# --- 0. ENVIRONMENT SETUP ---
# ------------------------------
SPARK_HOME = os.environ.get('SPARK_HOME', 'C:/Users/DELL/spark-3.5.7-bin-hadoop3')
HADOOP_ROOT_PATH = 'C:/hadoop'
os.environ['HADOOP_HOME'] = HADOOP_ROOT_PATH
os.environ['PATH'] += f";{HADOOP_ROOT_PATH}/bin"

LOCAL_TMP_DIR = "C:/tmp/spark"
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# ------------------------------
# --- 1. SPARK SESSION ---
# ------------------------------
packages = [
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
]

spark = (
    SparkSession.builder
    .appName("dim_order_items_etl")
    .master("local[*]")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
    .config("spark.sql.warehouse.dir", LOCAL_TMP_DIR)
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

# ------------------------------
# --- 2. PATHS ---
# ------------------------------
BUCKET_NAME = "customerorderstoredata"
stg_path = f"s3a://{BUCKET_NAME}/staging_data/order_items"
dim_path = f"s3a://{BUCKET_NAME}/Transform_data/Dimension/dim_order_items/"

# ------------------------------
# --- 3. LOAD STAGING DATA ---
# ------------------------------
try:
    stgdf = spark.read.parquet(stg_path)
    stgdf.printSchema()
except Exception as e:
    print(f"Error reading staging data: {e}")
    spark.stop()
    sys.exit(1)

# Remove duplicates and add SCD2 columns
stgdf = stgdf.dropDuplicates(["ORDER_ID", "LINE_ITEM_ID", "PRODUCT_ID"])
stgdf = (
    stgdf
    .withColumn("dim_order_item_sk", monotonically_increasing_id())
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast(TimestampType()))
    .withColumn("is_active", lit(1))
)

# ------------------------------
# --- 4. MERGE / SCD2 LOGIC ---
# ------------------------------
try:
    dim_table = DeltaTable.forPath(spark, dim_path)
    dim_exists = True
except:
    dim_exists = False

if dim_exists:
    dim_table.alias("target").merge(
        stgdf.alias("source"),
        """
        target.ORDER_ID = source.ORDER_ID AND
        target.LINE_ITEM_ID = source.LINE_ITEM_ID AND
        target.PRODUCT_ID = source.PRODUCT_ID AND
        target.is_active = 1
        """
    ).whenMatchedUpdate(
        condition="target.UNIT_PRICE <> source.UNIT_PRICE OR target.QUANTITY <> source.QUANTITY",
        set={"is_active": lit(0), "effective_to": current_timestamp()}
    ).whenNotMatchedInsertAll().execute()
else:
    stgdf.write.format("delta").mode("overwrite").save(dim_path)

# ------------------------------
# --- 5. VALIDATION ---
# ------------------------------
print("\n--- Validation after ETL ---")
dim_df = spark.read.format("delta").load(dim_path)
print(f"Total rows in dimension: {dim_df.count()}")
print("Sample active rows:")
dim_df.filter(col("is_active") == 1).show(5, truncate=False)

spark.stop()
print("\nETL completed successfully.")
