## 1) Imports
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

def print_bye():
    return 'Bye...'

## DAG definition
dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

## Tasks definition
hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
bye_operator = PythonOperator(task_id='bye_task', python_callable=print_bye, dag=dag)


## Task dependencies
hello_operator >> bye_operator