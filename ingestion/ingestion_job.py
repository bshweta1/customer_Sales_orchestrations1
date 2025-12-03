from pyspark.sql import SparkSession

from ingest_customers import ingest_customers
from ingest_Orders import ingest_orders
from ingest_products import ingest_products
from ingest_stores import ingest_stores
from ingest_orderitems import ingest_orderitems


def main():
    # Spark session for Glue
    spark = SparkSession.builder.appName("FullIngestionJob").getOrCreate()

    print("Starting ingestion workflow...")

    ingest_customers(spark)
    print("Customers done")

    ingest_orders(spark)
    print("Orders done")

    ingest_products(spark)
    print("Products done")

    ingest_orderitems(spark)
    print("orderitems done")

    ingest_stores(spark)
    print("Stores done")

    print("Ingestion workflow completed successfully!")


if __name__ == "__main__":
    main()
