# # import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp
#
# # --- Environment Setup ---
# # os.environ['SPARK_HOME'] = 'C:/Users/DELL/spark-3.5.7-bin-hadoop3'
# # os.environ['JAVA_HOME'] = 'C:/Progra~1/Eclipse Adoptium/jdk-11.0.29.7-hotspot'
# # os.environ['HADOOP_HOME'] = 'C:/hadoop'
# # os.environ['PATH'] += ';C:/hadoop/bin'
#
# S3_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262'
# # os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {S3_PACKAGES} --master local[*] pyspark-shell'
#
# # --- Spark Session ---
#
# def ingest_orderitems():
#     # ---------- Spark Session ----------
#     spark = SparkSession.builder.appName("Ingest_orderitems").getOrCreate()
#     BUCKET_NAME = "customerorderstoredata"
#
#     # spark = (
#     #     SparkSession.builder
#     #     .appName("IngestOrders")
#     #     .master("local[*]")
#     #     .config("spark.driver.bindAddress", "127.0.0.1")
#     #     .config("spark.driver.host", "127.0.0.1")
#     #     .config("spark.local.dir", "C:/tmp/spark")   # FIX: valid temp dir
#     #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     #     .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
#     #     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
#     #     .getOrCreate()
#     # )
#
#
#     # --- Read Raw Orders ---
#     raw_path = f"s3://{BUCKET_NAME}/Raw_data/order_items.txt"
#     df = spark.read.csv(raw_path, header=True, inferSchema=True)
#
#     # --- Add ingestion timestamp ---
#     df = df.withColumn("ingested_at", current_timestamp())
#
#     # --- Write to staging_data folder ---
#     staging_path = f"s3://{BUCKET_NAME}/staging_data/order_items"
#     df.write.mode("overwrite").parquet(staging_path)
#
#     print(f"Orderitems staged successfully at {staging_path}")
#
# if __name__ == "__main__":
#     ingest_orderitems()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def ingest_orderitems(spark=None):
    """
    Ingest order items from S3 CSV and write to staging parquet.

    Args:
        spark (SparkSession, optional): Spark session object. If None, creates a new one.
    """
    # Create Spark session if not provided
    if spark is None:
        spark = SparkSession.builder.appName("Ingest_orderitems").getOrCreate()

    BUCKET_NAME = "customerorderstoredata"
    raw_path = f"s3://{BUCKET_NAME}/Raw_data/order_items.txt"

    # Read CSV file
    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    # Add ingestion timestamp
    df = df.withColumn("ingested_at", current_timestamp())

    # Write to staging folder
    staging_path = f"s3://{BUCKET_NAME}/staging_data/order_items"
    df.write.mode("overwrite").parquet(staging_path)

    print(f"Orderitems staged successfully at {staging_path}")


# Make script callable directly
def main():
    ingest_orderitems()


if __name__ == "__main__":
    main()

