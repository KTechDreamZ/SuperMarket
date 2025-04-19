import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Constants
POSTGRES_CONN_ID = "project_connection"
SOURCE_TABLE = "sales_transformed_dag"
DIM_STORE_TABLE = "dim_store"

def create_db_engine():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_str = pg_hook.get_uri()
    engine = create_engine(conn_str, connect_args={"options": "-csearch_path=public"})
    return engine

def create_dim_store_table(engine):
    query = f"""
    CREATE TABLE IF NOT EXISTS {DIM_STORE_TABLE} (
        "Store ID" TEXT,
        "State" TEXT,
        "City" TEXT,
        "File Name" TEXT,
        "Load Date" DATE
    );
    """
    with engine.begin() as connection:
        connection.execute(query)
    logging.info("✅ dim_store table created or already exists.")

def load_dim_store():
    engine = create_db_engine()
    create_dim_store_table(engine)

    # Extract all product data (no DISTINCT)
    query = f"""
        SELECT 
            "Store ID", 
            "State", 
            "City",
            "File Name"
        FROM {SOURCE_TABLE}
        WHERE "Store ID" IS NOT NULL
    """
    df = pd.read_sql(query, engine)

    # Add load date to all rows
    df["Load Date"] = datetime.today().date()

    # Load all rows (no upsert, just append)
    df.to_sql(DIM_STORE_TABLE, engine, if_exists="append", index=False)

    logging.info("✅ dim_store table loaded with all rows (duplicates allowed).")

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dim_store",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

load_dim_store_task = PythonOperator(
    task_id="load_dim_store",
    python_callable=load_dim_store,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)