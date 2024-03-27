from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from orders_etl import main as etl_main

# Assuming orders_etl.py is in the same directory as your DAG
sys.path.append('/mnt/c/Users/d.lardizabal/Downloads/etl_python_sql/dags')

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
    'orders_delta_load', 
    default_args=default_args, 
    description='A simple DAG to ETL orders',
    schedule='@hourly',
    catchup=False
    )

run_etl = PythonOperator(
    task_id='run_etl_orders',
    python_callable=etl_main,  
    dag=dag,
)

run_etl