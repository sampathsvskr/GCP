
from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


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
    print(val[0])
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


t1 = DummyOperator(task_id="t1")
t2 = DummyOperator(task_id="t2")

select_barnch1 = BranchPythonOperator(task_id="select_barnch1", python_callable=branch_fn, op_args = [12])
branch_task1 = DummyOperator(task_id="branch_task1")
branch_task2 = DummyOperator(task_id="branch_task2")

select_barnch2 = BranchPythonOperator(task_id="select_barnch2", python_callable=branch_fn2, op_args = [5])
branch_task3 = DummyOperator(task_id="branch_task3")
branch_task4 = DummyOperator(task_id="branch_task4")

select_barnch3 = BranchPythonOperator(task_id="select_barnch3", python_callable=branch_fn3, op_args = [-2])
branch_task5 = DummyOperator(task_id="branch_task5")
branch_task6 = DummyOperator(task_id="branch_task6")


t5 = DummyOperator(task_id="t5")
t6 = DummyOperator(task_id="t6")
t7 = DummyOperator(task_id="t7")

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

select_barnch4 = BranchPythonOperator(task_id="select_barnch4", python_callable=branch_fn4)
branch_task7 = DummyOperator(task_id="branch_task7")
branch_task8 = DummyOperator(task_id="branch_task8")
branch_task9 = DummyOperator(task_id="branch_task9")
branch_task10 = DummyOperator(task_id="branch_task10")

t6 >> select_barnch4 >> [ branch_task7, branch_task8, branch_task9, branch_task10]
    



