import sys
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
from env_setup import get_spark, get_paths

# ------------------------------
# --- 0. SPARK SESSION & PATHS ---
# ------------------------------
spark=get_spark(app_name="dim_stores_etl")
stg_path , dim_path = get_paths(table_name="stores")

# ------------------------------
# --- 1. CHECK IF DIMENSION EXISTS ---
# ------------------------------
try:
    dim_table=DeltaTable.forPath(spark , dim_path)
    dim_exists =True
except:
    dim_exists=False

try:
    stgdf = spark.read.parquet(stg_path)
    stgdf.printSchema()
except Exception as e:
    print(f"Error reading staging data: {e}")
    spark.stop()
    sys.exit(1)

# Remove duplicates and add SCD2 columns

stgdf=stgdf.dropDuplicates(["STORE_ID","STORE_NAME"])
stgdf = (
    stgdf
    .withColumn("dim_store_sk", monotonically_increasing_id())
    .withColumn("effective_from", current_timestamp())
    .withColumn("effective_to", lit(None).cast(TimestampType()))
    .withColumn("is_active", lit(1))
)

# ------------------------------
# --- 2. MERGE / SCD2 LOGIC ---
# ------------------------------
if dim_exists:
    dim_table.alias("target").merge(
        stgdf.alias("source"),
        """
        target.STORE_ID = source.STORE_ID AND
        target.is_active = 1
        """
    ).whenMatchedUpdate(
        condition="target.STORE_NAME <> source.STORE_NAME OR target.WEB_ADDRESS <> source.WEB_ADDRESS OR target.LATITUDE <> source.LATITUDE OR target.LONGITUDE <> source.LONGITUDE",
        set={"is_active": lit(0), "effective_to": current_timestamp()}
    ).whenNotMatchedInsertAll().execute()
else:
    stgdf.write.format("delta").mode("overwrite").save(dim_path)

# ------------------------------
# --- 3. VALIDATION ---
# ------------------------------
print("\n--- Validation after ETL ---")
dim_df = spark.read.format("delta").load(dim_path)
print(f"Total rows in dimension: {dim_df.count()}")
print("Sample active rows:")
dim_df.filter(col("is_active") == 1).show(5, truncate=False)

spark.stop()
print("\nETL completed successfully.")