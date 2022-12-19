import logging
import requests
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

import site
site.addsitedir('/home/airflow/gcs/dags/')

from custom_operator import BigQueryCustomExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'retries': 0   
}
params ={"project":"qwiklabs-gcp-04-9249f60ca555"}


dag = DAG(
    dag_id="custom_operator_dag",
    default_args=default_args,
    schedule_interval=None, 
    start_date=days_ago(1),
    params= params
)
extract = BashOperator(
    task_id='task1',
    bash_command = "echo task1",
    dag=dag
)

custom1 = BigQueryCustomExecuteQueryOperator(
    task_id = "create_table_dataset",
    sql_file_path = "/home/airflow/gcs/dags/create.sql",
    sql_params = {"project": "qwiklabs-gcp-04-9249f60ca555" , "dataset":"test", "table":"test_table"},
    gcp_conn_id= "gcp_connection",
    dag=dag
)

custom2 = BigQueryCustomExecuteQueryOperator(
    task_id = "insert_to_table",
    sql_file_path = "/home/airflow/gcs/dags/insert.sql",
    sql_params = {"project": "qwiklabs-gcp-04-9249f60ca555" , "dataset":"test", "table":"test_table"},
    gcp_conn_id= "gcp_connection",
    dag=dag
)

custom3 = BigQueryCustomExecuteQueryOperator(
    task_id = "insert_to_table_using_query",
    query = "insert into `{project}.{dataset}.{table}`(id, name, location) values(11, 'aa', 'bglr'),(22, 'bb' , 'hybd')",
    sql_params = {"project": "qwiklabs-gcp-04-9249f60ca555" , "dataset":"test", "table":"test_table"},
    gcp_conn_id= "gcp_connection",
    dag=dag
)

extract >> custom1 >> custom2 >> custom3
