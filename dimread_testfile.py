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
import boto3

s3 = boto3.client("s3")
resp = s3.get_bucket_location(Bucket="your-bucket")
print(resp)

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
    .config("spark.hadoop.fs.s3a.region", "eu-north-1")
    .config("spark.sql.warehouse.dir", LOCAL_TMP_DIR)
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

BUCKET_NAME = "customerorderstoredata"

dim_path = f"s3a://{BUCKET_NAME}/Transform_data/Dimension/dim_orders/"
dim_df = spark.read.format("delta").load(dim_path)
dim_df.show(5)

dim_path = f"s3a://{BUCKET_NAME}/Transform_data/Dimension/dim_customers/"
dim_df = spark.read.format("delta").load(dim_path)
dim_df.show(5)
stgdf_orders=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/stores")

stgdf_orders.printSchema()