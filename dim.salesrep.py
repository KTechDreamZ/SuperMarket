import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

POSTGRES_CONN_ID = "project_connection"
SOURCE_TABLE = "sales_transformed_dag"
DIM_SALES_REP_TABLE = "dim_sales_rep"

def create_db_engine():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_str = pg_hook.get_uri()
    engine = create_engine(conn_str, connect_args={"options": "-csearch_path=public"})
    return engine

def create_dim_sales_rep_table(engine):
    query = f"""
    CREATE TABLE IF NOT EXISTS {DIM_SALES_REP_TABLE} (
        "Sales Rep ID" TEXT,
        "Sales Rep Name" TEXT,
        "File Name" TEXT,
        "Load Date" DATE
    );
    """
    with engine.begin() as connection:
        connection.execute(query)
    logging.info(" dim_sales_rep table created or already exists.")

def load_dim_store():
    engine = create_db_engine()
    create_dim_sales_rep_table(engine)

    query = f"""
        SELECT 
            "Sales Rep ID", 
            "Sales Rep Name", 
            "File Name"
        FROM {SOURCE_TABLE}
        WHERE "Sales Rep ID" IS NOT NULL
    """
    df = pd.read_sql(query, engine)

    df["Load Date"] = datetime.today().date()

    df.to_sql(DIM_SALES_REP_TABLE, engine, if_exists="append", index=False)

    logging.info(" dim_sales_rep table loaded with all rows (duplicates allowed).")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dim_sales_rep",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

load_dim_sales_rep_task = PythonOperator(
    task_id="load_dim_sales_rep",
    python_callable=load_dim_store,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)
