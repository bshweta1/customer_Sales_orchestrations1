import os
from itertools import groupby

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, column

# --- Environment Setup ---
os.environ['SPARK_HOME'] = 'C:/Users/DELL/spark-3.5.7-bin-hadoop3'
os.environ['JAVA_HOME'] = 'C:/Progra~1/Eclipse Adoptium/jdk-11.0.29.7-hotspot'
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['PATH'] += ';C:/hadoop/bin'

S3_PACKAGES = 'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262'
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {S3_PACKAGES} --master local[*] pyspark-shell'

# --- Spark Session ---
BUCKET_NAME = "customerorderstoredata"

spark = (
    SparkSession.builder
    .appName("IngestOrders")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.local.dir", "C:/tmp/spark")   # FIX: valid temp dir
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

#-----schema validation
stgdf_Orderitems=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/order_items")
stgdf_Orderitems.printSchema()

stgdf_orders=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/orders")

stgdf_orders.printSchema()

stgdf_customers=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/customers")
stgdf_customers.printSchema()

stgdf_stores=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/stores")

stgdf_stores.printSchema()

stgdf_products=spark.read.parquet(f"s3a://{BUCKET_NAME}/staging_data/products")
stgdf_products.printSchema()


#--------------------null values
null_ordersdf=stgdf_orders.filter(column("ORDER_ID").isNull()).count()
print(f" undefined orderid numbers are:{null_ordersdf}")

null_orderItemsdf=stgdf_Orderitems.filter(column("Order_id").isNull()).count()
print(f" undefined orderitemsid numbers are:{null_orderItemsdf}")

null_customersdf=stgdf_customers.filter(column("CUSTOMER_ID").isNull()).count()
print(f" undefined customerid numbers are:{null_customersdf}")

null_productsdf=stgdf_products.filter(column("PRODUCT_ID").isNull()).count()
print(f" undefined productid numbers are:{null_productsdf}")

null_storesdf=stgdf_stores.filter(column("STORE_ID").isNull()).count()
print(f" undefined storesid numbers are:{null_storesdf}")

#-------------------check duplicates
# stgdf_Orderitems.show(50)

duplicate_OrderItems=stgdf_Orderitems.groupBy("ORDER_ID","LINE_ITEM_ID","PRODUCT_ID").count()\
                     .filter("count>1")
duplicate_OrderItems=duplicate_OrderItems.count()
print(f" duplicate orderitems are:{duplicate_OrderItems}")

duplicate_Orders=stgdf_orders.groupBy("ORDER_ID","CUSTOMER_ID","ORDER_DATETIME","STORE_ID").count() \
    .filter("count>1")
duplicate_Orders=duplicate_Orders.count()
print(f" duplicate orders are:{duplicate_Orders}")

duplicate_customers=stgdf_customers.groupBy("CUSTOMER_ID","EMAIL_ADDRESS").count() \
    .filter("count>1")
duplicate_customers=duplicate_customers.count()
print(f" duplicate customers are:{duplicate_Orders}")

duplicate_products=stgdf_products.groupBy("PRODUCT_ID","PRODUCT_NAME").count() \
    .filter("count>1")
duplicate_products=duplicate_products.count()
print(f" duplicate products are:{duplicate_Orders}")

duplicate_stores=stgdf_stores.groupBy("STORE_ID","STORE_NAME").count() \
    .filter("count>1")
duplicate_stores=duplicate_stores.count()
print(f" duplicate stores are:{duplicate_Orders}")

#----------------------srcCount,trgtCount--------------------------------
raw_orderitemsdf=spark.read.csv(f"s3a://{BUCKET_NAME}/Raw_data/order_items.txt",header="True",inferSchema="true")

rawdf_orderitemscount = raw_orderitemsdf.count()

stgdf_orderitemscount=stgdf_Orderitems.count()

raw_ordersdf=spark.read.parquet(f"s3a://{BUCKET_NAME}/Raw_data/orders.parquet")
rawdf_ordercount = raw_ordersdf.count()

stgdf_orderscount=stgdf_orders.count()

raw_customersdf=spark.read.json(f"s3a://{BUCKET_NAME}/Raw_data/customers.json")
rawdf_customerscount = raw_customersdf.count()

stgdf_customerscount=stgdf_customers.count()

raw_productsdf=spark.read.json(f"s3a://{BUCKET_NAME}/Raw_data/products.json")
rawdf_productcount = raw_productsdf.count()

stgdf_productscount=stgdf_products.count()

raw_storesdf=spark.read.json(f"s3a://{BUCKET_NAME}/Raw_data/stores.json")
rawdf_storescount = raw_storesdf.count()

stgdf_storescount=stgdf_stores.count()


# Compare orderItems
if rawdf_orderitemscount == stgdf_orderitemscount:
    print("Raw orderItems data is  matching staging orderItems data")

else:
    print("Raw orderItems data is NOT matching staging orderItems data")
    print(f"rawcount:{raw_orderitemsdf} and stgcount:{stgdf_orderitemscount}")

# Compare orders
if rawdf_ordercount == stgdf_orderscount:
    print("Raw orders data is matching staging orders data")
else:
    print("Raw orders data is NOT matching staging orders data")
    print(f"rawcount:{raw_ordersdf} and stgcount:{stgdf_orderscount}")

# Compare customers
if rawdf_customerscount == stgdf_customerscount:
    print("Raw customers data is matching staging customers data")
else:
    print("Raw customers data is NOT matching staging customers data")
    print(f"rawcount:{raw_customersdf} and stgcount:{stgdf_customers}")
# Compare products
if rawdf_productcount == stgdf_productscount:
    print("Raw products data is matching staging products data")
else:
    print("Raw products data is NOT matching staging products data")
    print(f"rawcount:{raw_productsdf} and stgcount:{stgdf_products}")
# Compare stores
if rawdf_storescount == stgdf_storescount:
    print("Raw stores data is matching staging stores data")
else:
    print("Raw stores data is NOT matching staging stores data")
    print(f"rawcount:{raw_storesdf} and stgcount:{stgdf_stores}")