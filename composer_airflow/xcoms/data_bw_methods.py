import logging
import requests
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 0   
}
def _extract_data():
    return ["a","b","c"]
def _transform_data(data):
    print(data)
    return data
def _load_results(data):
    print(data)
    print(f"data :", data)

dag = DAG(
    dag_id="exchange_data_bw_operators1",
    default_args=default_args,
    schedule_interval=None, 
    start_date=days_ago(1),
)
extract = PythonOperator(
    task_id='extract',
    python_callable=_extract_data,
    dag=dag
)
transform = PythonOperator(
    task_id='transform_data',
    python_callable=_transform_data,
    op_args=[extract],
    dag=dag
)
load = PythonOperator(
    task_id='load_data',
    python_callable=_load_results,
    op_args=[transform],
    dag=dag
)
extract >> transform >> load
