from env_setup import get_paths

def load_dimensions(spark, bucket="customerorderstoredata"):
    tables = ["customers", "products", "stores", "orders", "order_items"]
    dims = {}

    for table in tables:
        _, dim_path = get_paths(bucket_name=bucket, table_name=table)
        dims[f"dim_{table}"] = spark.read.format("delta").load(dim_path)

    return dims
