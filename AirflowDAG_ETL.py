from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import sys
import os
import logging

# ------------------------------
# Helper function to run scripts
# ------------------------------
def run_script(path, **kwargs):
    logging.info(f"Running script: {path}")
    spec = importlib.util.spec_from_file_location("module.name", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["module.name"] = module
    spec.loader.exec_module(module)
    logging.info(f"Finished script: {path}")

# ------------------------------
# Paths
# ------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

INGEST_PATHS = {
    "ingest_customers": os.path.join(BASE_DIR, "ingestion", "ingest_customers.py"),
    "ingest_order_items": os.path.join(BASE_DIR, "ingestion", "ingest_Orderlterms.py"),
    "ingest_orders": os.path.join(BASE_DIR, "ingestion", "ingest_Orders.py"),
    "ingest_products": os.path.join(BASE_DIR, "ingestion", "ingest_products.py"),
    "ingest_stores": os.path.join(BASE_DIR, "ingestion", "ingest_stores.py")
}

VALIDATE_PATH = os.path.join(BASE_DIR, "validate_data", "validata_data.py")

DIMENSION_SCRIPTS = {
    "dim_customers": os.path.join(BASE_DIR, "Dimensions", "Dim_Customers.py"),
    "dim_orderitems": os.path.join(BASE_DIR, "Dimensions", "Dim_orderItems.py"),
    "dim_orders": os.path.join(BASE_DIR, "Dimensions", "Dim_Orders.py"),
    "dim_products": os.path.join(BASE_DIR, "Dimensions", "Dim_Products.py"),
    "dim_stores": os.path.join(BASE_DIR, "Dimensions", "Dim_Stores.py"),
}

FACT_SCRIPTS = {
    "fact_customerSales": {
        "path": os.path.join(BASE_DIR, "Facts", "fact_customerSales.py"),
        "depends_on": ["dim_customers", "dim_products", "dim_stores", "dim_orders", "dim_orderitems"]
    },
    "fact_dailySalesSummary": {
        "path": os.path.join(BASE_DIR, "Facts", "fact_dailySalesSummary.py"),
        "depends_on": ["dim_orders", "dim_orderitems", "dim_stores"]
    },
    "fact_sales": {
        "path": os.path.join(BASE_DIR, "Facts", "fact_sales.py"),
        "depends_on": ["dim_orders", "dim_orderitems"]
    },
}

# ------------------------------
# DAG args
# ------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1),
    "email": ["youremail@example.com"],  # Update to your email
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

dag = DAG(
    "customer_sales_etl_prod",
    default_args=default_args,
    description="Production-ready ETL pipeline for Customer Sales",
    schedule_interval=None,
    max_active_runs=1,  # Prevent multiple concurrent runs
)

# ------------------------------
# Tasks
# ------------------------------

# Ingestion tasks
ingest_tasks = {
    name: PythonOperator(
        task_id=name,
        python_callable=run_script,
        op_kwargs={"path": path},
        dag=dag,
        pool="spark_jobs",  # optional: use a pool to limit concurrency
    ) for name, path in INGEST_PATHS.items()
}

# Validation task
validate_task = PythonOperator(
    task_id="validate_data",
    python_callable=run_script,
    op_kwargs={"path": VALIDATE_PATH},
    dag=dag,
    pool="spark_jobs",
)

# Dimension tasks
dim_tasks = {
    name: PythonOperator(
        task_id=name,
        python_callable=run_script,
        op_kwargs={"path": path},
        dag=dag,
        pool="spark_jobs",
    ) for name, path in DIMENSION_SCRIPTS.items()
}

# Fact tasks
fact_tasks = {}
for fact_name, fact_info in FACT_SCRIPTS.items():
    task = PythonOperator(
        task_id=fact_name,
        python_callable=run_script,
        op_kwargs={"path": fact_info["path"]},
        dag=dag,
        pool="spark_jobs",
    )
    fact_tasks[fact_name] = task

# ------------------------------
# Dependencies
# ------------------------------

# Ingest -> Validate
for task in ingest_tasks.values():
    task >> validate_task

# Validate -> Dimensions
for task in dim_tasks.values():
    validate_task >> task

# Dimensions -> Facts (only relevant dims)
for fact_name, fact_info in FACT_SCRIPTS.items():
    for dim_name in fact_info["depends_on"]:
        dim_tasks[dim_name] >> fact_tasks[fact_name]
