# env_setup.py
import os
from pyspark.sql import SparkSession

def get_spark(app_name="etl_app", local_tmp_dir="C:/tmp/spark"):
    """
    Returns a SparkSession configured with Delta Lake and AWS S3 support.
    """
    SPARK_HOME = os.environ.get('SPARK_HOME', 'C:/Users/DELL/spark-3.5.7-bin-hadoop3')
    HADOOP_ROOT_PATH = 'C:/hadoop'
    os.environ['HADOOP_HOME'] = HADOOP_ROOT_PATH
    os.environ['PATH'] += f";{HADOOP_ROOT_PATH}/bin"

    os.makedirs(local_tmp_dir, exist_ok=True)

    packages = [
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
        .config("spark.sql.warehouse.dir", local_tmp_dir)
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_paths(bucket_name="customerorderstoredata", table_name="order_items"):
    """
    Returns staging and dimension paths for a given table dynamically.
    """
    stg_path = f"s3a://{bucket_name}/staging_data/{table_name}"
    dim_path = f"s3a://{bucket_name}/Transform_data/Dimension/dim_{table_name}"
    return stg_path, dim_path

