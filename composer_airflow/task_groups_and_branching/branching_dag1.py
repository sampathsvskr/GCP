from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,
    'email_on_failure': False,
    'email':["test@gmail.com"],
    'email_on_retry': False 
    
}


dag = DAG("branching_test_dag1",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )


def branch_fn(val):
    print(val)
    
    if val>10:
        return "branch_task1"
    else:
        return "branch_task2"

t1 = EmptyOperator(task_id="t1", dag=dag)
select_barnch1 = BranchPythonOperator(task_id="select_barnch1", python_callable=branch_fn, op_args = [12], dag=dag)
branch_task1 = EmptyOperator(task_id="branch_task1", dag=dag , )
branch_task2 = EmptyOperator(task_id="branch_task2", dag=dag)
t2 = EmptyOperator(task_id="t2", dag=dag , trigger_rule=TriggerRule.ONE_SUCCESS)

t1 >> select_barnch1 >> [branch_task1,branch_task2] >> t2