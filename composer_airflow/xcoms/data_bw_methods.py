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
    return {'no_records': len(data)}
def _load_results(no_records):
    print(f"no. of records = {no_records}")

with DAG(
    dag_id="exchange_data_bw_operators",
    default_args=default_args,
    schedule_interval='@hourly', 
    start_date=days_ago(1)
):
    extract = PythonOperator(
        task_id='extract_from_api',
        python_callable=_extract_data,
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=_transform_data,
        op_kwargs=[extract]
    )
    load = PythonOperator(
        task_id='load_data',
        python_callable=_load_results,
        op_kwargs=[transform['no_records']]
    )
    extract >> transform >> load
