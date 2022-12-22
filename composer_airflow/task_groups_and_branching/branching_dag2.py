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


dag = DAG("branching_test_dag2",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )


def branch_fn4():
    val =10
    if val:
        return ["branch_task7","branch_task8"]
    else:
        return ["branch_task9","branch_task10"]

t1 = EmptyOperator(task_id="t1", dag=dag)
select_barnch4 = BranchPythonOperator(task_id="select_barnch4", python_callable=branch_fn4 , dag=dag)
branch_task7 = EmptyOperator(task_id="branch_task7", dag=dag)
branch_task8 = EmptyOperator(task_id="branch_task8", dag=dag)
branch_task9 = EmptyOperator(task_id="branch_task9", dag=dag)
branch_task10 = EmptyOperator(task_id="branch_task10", dag=dag)
t2 = EmptyOperator(task_id="t2", dag=dag , trigger_rule=TriggerRule.ONE_SUCCESS)

t1 >> select_barnch4 
select_barnch4 >> branch_task7 >> branch_task8 >> t2
select_barnch4 >> [branch_task9,branch_task10] >> t2