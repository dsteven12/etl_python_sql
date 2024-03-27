from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from orders_etl import main as etl_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email': ['dsteven12@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_orders', 
    default_args=default_args, 
    description='A simple DAG to ETL orders',
    schedule_interval='@hourly',
    catchup=False
    )

run_etl = PythonOperator(
    task_id='run_etl_orders',
    python_callable=etl_main,  # Directly reference the imported function
    dag=dag,
)