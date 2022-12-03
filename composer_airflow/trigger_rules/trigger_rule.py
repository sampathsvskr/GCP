'''
Trigger Rule -- > trigger_rule
 All upstream tasks are in a failed or upstream_failed state
'''

from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

def create_dag(trigger_rule):

    #sample methods
    def success_fun():
        pass

    def failure_fun():
        print(10/0)

    #to mark task to skipped state 
    def skip_task():
        raise AirflowSkipException
        
    dag = DAG(f"{trigger_rule}_trigger_rule_dag",            
            schedule_interval=None,
            start_date=datetime(2022,12,1)
            )
            
    # trigger_rule: All upstream tasks are in a failed or upstream_failed state

    '''
    case-1
    upstream task --> success(task2)
    '''
    task2 = PythonOperator(task_id="task2", python_callable=success_fun, dag=dag)
    task3 = PythonOperator(task_id="task3", python_callable=success_fun, dag=dag, trigger_rule =trigger_rule)

    task2  >> task3

    '''
    case-2
    upstream task --> failed(task4)
    '''
    task4 = PythonOperator(task_id="task4", python_callable=failure_fun, dag=dag)
    task5 = PythonOperator(task_id="task5", python_callable=success_fun, dag=dag, trigger_rule =trigger_rule)

    task4 >> task5 


    '''
    case-3
    upstream task --> upstream failed(task7)
    '''
    task6 = PythonOperator(task_id="task6", python_callable=failure_fun, dag=dag)
    task7 = PythonOperator(task_id="task7", python_callable=success_fun, dag=dag)
    task8 = PythonOperator(task_id="task8", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task6 >> task7 >> task8


    '''
    case-4
    upstream task --> skipped(task10)
    '''

    task9 = PythonOperator(task_id="task9", python_callable=skip_task, dag=dag)
    task10 = PythonOperator(task_id="task10", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task9 >> task10

    '''
    case-5
    upstream task --> success(task11,task12)
    '''
    task11 = PythonOperator(task_id="task11", python_callable=success_fun, dag=dag)
    task12 = PythonOperator(task_id="task12", python_callable=success_fun, dag=dag)
    task13 = PythonOperator(task_id="task13", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [task11 , task12] >> task13

    '''
    case-6
    upstream task --> success,failed(task14,task15)
    '''
    task14 = PythonOperator(task_id="task14", python_callable=failure_fun, dag=dag)
    task15 = PythonOperator(task_id="task15", python_callable=success_fun, dag=dag)
    task16 = PythonOperator(task_id="task16", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task14 >> task15 >> task16

    '''
    case-7
    upstream task --> failed(task17,task18)
    '''
    task17 = PythonOperator(task_id="task17", python_callable=failure_fun, dag=dag)
    task18 = PythonOperator(task_id="task18", python_callable=failure_fun, dag=dag)
    task19 = PythonOperator(task_id="task19", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)


    [task17 , task18] >> task19

    '''
    case-8 
    upstream task --> skipped(task20,task21)
    '''
    task20 = PythonOperator(task_id="task20", python_callable=skip_task, dag=dag)
    task21 = PythonOperator(task_id="task21", python_callable=skip_task, dag=dag)
    task22 = PythonOperator(task_id="task22", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [task20 , task21] >> task22

    '''
    case-9
    upstream task --> success, skip(task23,task24)
    '''
    task23 = PythonOperator(task_id="task23", python_callable=success_fun, dag=dag)
    task24 = PythonOperator(task_id="task24", python_callable=skip_task, dag=dag)
    task25 = PythonOperator(task_id="task25", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [task23 , task24] >> task25

    '''
    case-10
    upstream task --> failed, skip(task26,task27)
    '''
    task26 = PythonOperator(task_id="task26", python_callable=failure_fun, dag=dag)
    task27 = PythonOperator(task_id="task27", python_callable=skip_task, dag=dag)
    task28 = PythonOperator(task_id="task28", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [task26 , task27] >> task28

    '''
    case-11
    upstream task --> upstream_failed(task29,task30)
    '''
    task32= PythonOperator(task_id="task32", python_callable=failure_fun, dag=dag)
    task29 = PythonOperator(task_id="task29", python_callable=success_fun, dag=dag)
    task30 = PythonOperator(task_id="task30", python_callable=success_fun, dag=dag)
    task31 = PythonOperator(task_id="task31", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task32 >> [task29 , task30] >> task31

    '''
    case-12
    upstream task --> upstream_failed, success(task29,task30)
    '''
    task33= PythonOperator(task_id="task33", python_callable=success_fun, dag=dag)
    task34 = PythonOperator(task_id="task34", python_callable=success_fun, dag=dag, trigger_rule="all_failed")
    task35 = PythonOperator(task_id="task35", python_callable=success_fun, dag=dag)
    task36 = PythonOperator(task_id="task36", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task33 >> [task34 , task35] >> task36


    '''
    case-13
    upstream task --> upstream_failed, failed(task38,task39)
    '''
    task37= PythonOperator(task_id="task37", python_callable=success_fun, dag=dag)
    task38 = PythonOperator(task_id="task38", python_callable=failure_fun, dag=dag, trigger_rule="all_failed")
    task39 = PythonOperator(task_id="task39", python_callable=success_fun, dag=dag)
    task40 = PythonOperator(task_id="task40", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task37 >> [task38 , task39] >> task40

    '''
    case-12
    upstream task --> upstream_failed, skip(task42,task43)
    '''
    task41= PythonOperator(task_id="task41", python_callable=failure_fun, dag=dag)
    task42 = PythonOperator(task_id="task42", python_callable=skip_task, dag=dag, trigger_rule="all_failed")
    task43 = PythonOperator(task_id="task43", python_callable=success_fun, dag=dag)
    task44 = PythonOperator(task_id="task44", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task41 >> [task42 , task43] >> task44


    '''
    case-13
    upstream task --> upstream_failed, skip, failed, success(task46,47,48,49)
    '''
    task45 = PythonOperator(task_id="task45", python_callable=failure_fun, dag=dag)
    task46 = PythonOperator(task_id="task46", python_callable=skip_task, dag=dag, trigger_rule="all_failed")
    task47 = PythonOperator(task_id="task47", python_callable=success_fun, dag=dag, trigger_rule="all_failed")
    task48 = PythonOperator(task_id="task48", python_callable=failure_fun, dag=dag, trigger_rule="all_failed")
    task49 = PythonOperator(task_id="task49", python_callable=success_fun, dag=dag)
    task50 = PythonOperator(task_id="task50", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    task45 >> [task46 , task47, task48, task49] >> task50


    return dag


trigger_rules = ["all_success","all_failed","all_done","all_skipped","one_failed","one_success","one_done","none_failed","none_failed_min_one_success","none_skipped","always"]

for trigger_rule in trigger_rules:
    globals()[trigger_rule]=create_dag(trigger_rule)

