from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sub_module import extract_and_load

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 22),
    "catchup": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="sales_extraction",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

extract_load_task = PythonOperator(
    task_id="extract_and_load",
    python_callable=extract_and_load,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

extract_load_task
