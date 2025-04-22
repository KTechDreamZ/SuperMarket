import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

POSTGRES_CONN_ID = "project_connection"
SOURCE_TABLE = "sales_transformed_dag"
DIM_WHOLESALER_TABLE = "dim_wholesaler"

def create_db_engine():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_str = pg_hook.get_uri()
    engine = create_engine(conn_str, connect_args={"options": "-csearch_path=public"})
    return engine

def create_dim_wholesaler_table(engine):
    query = f"""
    CREATE TABLE IF NOT EXISTS {DIM_WHOLESALER_TABLE} (
        "Wholesaler ID" TEXT,
        "Wholesaler Name" TEXT,
        "File Name" TEXT,
        "Load Date" DATE
    );
    """
    with engine.begin() as connection:
        connection.execute(query)
    logging.info(" dim_wholesaler table created or already exists.")

def load_dim_wholesaler():
    engine = create_db_engine()
    create_dim_wholesaler_table(engine)

    query = f"""
        SELECT 
            "Wholesaler ID",
            "Wholesaler Name",
            "File Name"
        FROM {SOURCE_TABLE}
        WHERE "Wholesaler ID" IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    df["Load Date"] = datetime.today().date()

    df.to_sql(DIM_WHOLESALER_TABLE, engine, if_exists="append", index=False)
    logging.info(" dim_wholesaler table loaded.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="dim_wholesaler",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

load_dim_wholesaler_task = PythonOperator(
    task_id="load_dim_wholesaler",
    python_callable=load_dim_wholesaler,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)
