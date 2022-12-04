
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


dag = DAG("branching_test_dag",
           #schedule_interval=None,
           catchup=True,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )


def branch_fn(val):
    print(val)
    
    if val>10:
        return "branch_task1"
    else:
        return "branch_task2"
    

def branch_fn2(val):
    print(val)
    if val>10:
        return "branch_task3"
    else:
        return "branch_task4"

def branch_fn3(val):
    print(val)
    if val>10:
        return "branch_task1"
    elif val <=10 and val >0:
        return "branch_task2"
    else:
        print("No branch..")


t1 = EmptyOperator(task_id="t1", dag=dag)

''' 
select_barnch1 >> [branch_task1,branch_task2] >> t2
Since there is branch task before t2, which returns either branch1 or branch2. 
So, only one of branch1 or branch2 task will be excuted and other will be skipped.
By setting trigger rule as "one_success" to t2, indicates trigger this task if atleast one task is success.
'''
t2 = EmptyOperator(task_id="t2", dag=dag,trigger_rule=TriggerRule.ONE_SUCCESS)

select_barnch1 = BranchPythonOperator(task_id="select_barnch1", python_callable=branch_fn, op_args = [12], dag=dag)
branch_task1 = EmptyOperator(task_id="branch_task1", dag=dag)
branch_task2 = EmptyOperator(task_id="branch_task2", dag=dag)

select_barnch2 = BranchPythonOperator(task_id="select_barnch2", python_callable=branch_fn2, op_args = [5], dag=dag)
branch_task3 = EmptyOperator(task_id="branch_task3" , dag=dag)
branch_task4 = EmptyOperator(task_id="branch_task4", dag=dag)

select_barnch3 = BranchPythonOperator(task_id="select_barnch3", python_callable=branch_fn3, op_args = [-2] , dag=dag)
branch_task5 = EmptyOperator(task_id="branch_task5", dag=dag)
branch_task6 = EmptyOperator(task_id="branch_task6", dag=dag)


t5 = EmptyOperator(task_id="t5", dag=dag)

''' 
select_barnch3 >> [branch_task5,branch_task6] >> t6
In select_barnch3 task arg, negative number is given, so the callable fn doesn't return any branching task
So, here both the branch tasks will be skipped 
By setting trigger rule as "ALL_SKIPPED" to t6, indicates if both taks are skipped, trigger it.
'''
t6 = EmptyOperator(task_id="t6", dag=dag,trigger_rule=TriggerRule.ALL_SKIPPED)

''' 
select_barnch2 >> branch_task4 >> t5 >> t7
Since there is branch task before t7, which returns either branch3 or branch4. 
So, only one of branch1 or branch2 task will be excuted and other will be skipped.
By setting trigger rule as "one_success" to t7, indicates trigger this task if atleast one task is success
'''
t7 = EmptyOperator(task_id="t7", dag=dag,trigger_rule=TriggerRule.ONE_SUCCESS)

t1 >> select_barnch1 >> [branch_task1,branch_task2] >> t2 >> select_barnch2

select_barnch2 >> branch_task3 >> t7
select_barnch2 >> branch_task4 >> t5 >> t7

t7 >> select_barnch3 >> [branch_task5,branch_task6] >> t6


def branch_fn4():
    val =10
    if val:
        return ["branch_task7","branch_task8"]
    else:
        return ["branch_task9","branch_task10"]

select_barnch4 = BranchPythonOperator(task_id="select_barnch4", python_callable=branch_fn4 , dag=dag)
branch_task7 = EmptyOperator(task_id="branch_task7", dag=dag)
branch_task8 = EmptyOperator(task_id="branch_task8", dag=dag)
branch_task9 = EmptyOperator(task_id="branch_task9", dag=dag)
branch_task10 = EmptyOperator(task_id="branch_task10", dag=dag)

t6 >> select_barnch4 >> [ branch_task7, branch_task8, branch_task9, branch_task10]
    




