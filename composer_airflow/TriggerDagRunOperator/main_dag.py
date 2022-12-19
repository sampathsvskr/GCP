from airflow import DAG
from airflow.utils import dates
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {
    'start_date': dates.days_ago(2)
}
   

with DAG('main_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    start_dag = BashOperator(
        task_id='start_dag',
        bash_command='echo trigger another dag'
    )

    trigger_dag_1 = TriggerDagRunOperator(
        task_id='trigger_sub_dag', 
        trigger_dag_id= "sub_dag",
        execution_date = '{{ ds }}' , ## to set the same execution date as main dag for sub dag
        reset_dag_run = True, ##  Whether or not clear existing dag run if already exists
        wait_for_completion = True, ## Wait until this dag execution is completed.
        poke_interval = 10 ## Poke interval to check dag run status when wait_for_completion=True. (default: 60)
    )

    end_dag = BashOperator(
        task_id='start_dag',
        bash_command='echo done..'
    )


    start_dag >> trigger_dag_1 >> end_dag