# transform_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transform_module import transform_and_load

# Default DAG settings
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 22),
    "catchup": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    dag_id="sales_transformation",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Task Definition
transform_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_and_load,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

transform_load_task
