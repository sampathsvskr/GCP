from airflow import DAG
from airflow.utils import dates
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'start_date': dates.days_ago(2)
}
   
def _sample_fun():
    return "success"

with DAG('sub_dag1', 
    schedule_interval=None, 
    default_args=default_args, 
    catchup=False) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='sleep 30'
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=_sample_fun
    )

    task1 >> task2