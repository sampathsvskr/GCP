'''
Creating task groups
'''

from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

#sample methods
def print_hello():
    return "Hello, welcome to airflow.."

def test():
    time.sleep(10)
    return "Slept for 10 seconds..."
    
dag = DAG("task_grp_dag", 
           description="testing task grouping",
           schedule_interval=None,
           start_date=datetime(2022,12,12)
           )
           
task1= PythonOperator(task_id="task1", python_callable=print_hello, dag=dag)

## parallel task group
with TaskGroup('task_group_parallel', tooltip='task_group_parallel', dag=dag) as task_group_parallel:
    parallel1= PythonOperator(task_id="parallel1", python_callable=print_hello, dag=dag)
    parallel2= PythonOperator(task_id="parallel2", python_callable=test, dag=dag)
    
task2= PythonOperator(task_id="task2", python_callable=print_hello, dag=dag)

## series task group
with TaskGroup('task_group_series', tooltip='task_group_series', dag=dag) as task_group_series:
    series1= PythonOperator(task_id="series1", python_callable=print_hello, dag=dag)
    series2= PythonOperator(task_id="series2", python_callable=test, dag=dag)
    
    series2 >> series1
    
task3 = PythonOperator(task_id="task3", python_callable=print_hello, dag=dag)

## nested task group
with TaskGroup('task_group_nested', tooltip='task_group_nested', dag=dag) as task_group_nested:
    nested1= PythonOperator(task_id="nested1", python_callable=print_hello, dag=dag)
    nested2= PythonOperator(task_id="nested2", python_callable=test, dag=dag)

    with TaskGroup('sub_task_grp', tooltip='sub_task_grp', dag=dag) as sub_task_grp:
        sub_task_grp1= PythonOperator(task_id="sub_task_grp1", python_callable=print_hello, dag=dag)
        sub_task_grp2= PythonOperator(task_id="sub_task_grp2", python_callable=test, dag=dag)
        
        sub_task_grp1 >> sub_task_grp2

## nested with some random ordering of tasks
with TaskGroup('task_group_nested2', tooltip='task_group_nested2', dag=dag) as task_group_nested2:
    nested1= PythonOperator(task_id="nested1", python_callable=print_hello, dag=dag)
    nested2= PythonOperator(task_id="nested2", python_callable=test, dag=dag)

    with TaskGroup('sub_task_grp', tooltip='sub_task_grp', dag=dag) as sub_task_grp:
        sub_task_grp1= PythonOperator(task_id="sub_task_grp1", python_callable=print_hello, dag=dag)
        sub_task_grp2= PythonOperator(task_id="sub_task_grp2", python_callable=test, dag=dag)
        
        sub_task_grp1 >> sub_task_grp2

    sub_task_grp >> [nested1,nested2] >> nested1 >> nested2
    
task4= PythonOperator(task_id="task4", python_callable=test, dag=dag)
 

task1 >> task_group_parallel >> task2 >> task_group_series >> task3 >> task_group_nested >> task_group_nested2 >> task4
    
    