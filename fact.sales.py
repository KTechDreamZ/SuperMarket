import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

POSTGRES_CONN_ID = "project_connection"
SOURCE_TABLE = "sales_transformed_dag"
FACT_SALES_TABLE = "fact_sales"

def create_db_engine():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_str = pg_hook.get_uri()
    engine = create_engine(conn_str, connect_args={"options": "-csearch_path=public"})
    return engine

def create_fact_sales_table(engine):
    query = f"""
    CREATE TABLE IF NOT EXISTS {FACT_SALES_TABLE} (
        "Transaction ID" TEXT PRIMARY KEY,
        "Product ID" TEXT,
        "Wholesaler ID" TEXT,
        "Store ID" TEXT,
        "Total Sales Amount" NUMERIC,
        "Units Sold" INTEGER,
        "GST Total Amount" NUMERIC,
        "Transaction Date" DATE,
        "Sales Type" TEXT,
        "Payment Method" TEXT,
        "File Name" TEXT,
        "Load Date" DATE
    );
    """
    with engine.begin() as connection:
        connection.execute(query)
    logging.info("✅ fact_sales table created or already exists.")

def load_fact_sales():
    engine = create_db_engine()
    create_fact_sales_table(engine)

    query = f"""
        SELECT 
            "Transaction ID",
            "Product ID",
            "Wholesaler ID",
            "Store ID",
            CAST("Total Sales Amount" AS NUMERIC),
            CAST("Units Sold" AS INTEGER),
            CAST("GST Total Amount" AS NUMERIC),
            CAST("Transaction Date" AS DATE),
            "Sales Type",
            "Payment Method",
            "File Name"
        FROM {SOURCE_TABLE}
    """
    df = pd.read_sql(query, engine)
    df["Load Date"] = datetime.today().date()

    df.to_sql(FACT_SALES_TABLE, engine, if_exists="append", index=False)
    logging.info("✅ fact_sales table loaded.")

# DAG definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="fact_sales",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

load_fact_sales_task = PythonOperator(
    task_id="load_fact_sales",
    python_callable=load_fact_sales,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)
