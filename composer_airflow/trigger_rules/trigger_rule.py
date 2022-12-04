'''
Trigger Rule -- > trigger_rule
 All upstream tasks are in a failed or upstream_failed state
The trigger_rule must be one of 
{
    <TriggerRule.ALL_SUCCESS: 'all_success'>, 
    <TriggerRule.DUMMY: 'dummy'>, 
    <TriggerRule.NONE_FAILED_OR_SKIPPED: 'none_failed_or_skipped'>, 
    <TriggerRule.ALWAYS: 'always'>, 
    <TriggerRule.ALL_SKIPPED: 'all_skipped'>, 
    <TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS: 'none_failed_min_one_success'>, 
    <TriggerRule.NONE_FAILED: 'none_failed'>, 
    <TriggerRule.NONE_SKIPPED: 'none_skipped'>, 
    <TriggerRule.ALL_FAILED: 'all_failed'>, 
    <TriggerRule.ALL_DONE: 'all_done'>, 
    <TriggerRule.ONE_FAILED: 'one_failed'>, 
    <TriggerRule.ONE_SUCCESS: 'one_success'>}

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

    default_args= {   
        
        'retries': 0
        
        
    }
        
    dag = DAG(f"{trigger_rule}_trigger_rule_dag1",            
            #schedule_interval=None,
            start_date=datetime(2022,12,1),
            default_args=default_args
            )
            
    # trigger_rule: All upstream tasks are in a failed or upstream_failed state

    '''
    case-1
    upstream task --> success(task2)
    '''
    success_fun2 = PythonOperator(task_id="success_fun2", python_callable=success_fun, dag=dag)
    task3 = PythonOperator(task_id="task3", python_callable=success_fun, dag=dag, trigger_rule =trigger_rule)

    success_fun2  >> task3

    '''
    case-2
    upstream task --> failed(task4)
    '''
    failure_fun4 = PythonOperator(task_id="failure_fun4", python_callable=failure_fun, dag=dag)
    task5 = PythonOperator(task_id="task5", python_callable=success_fun, dag=dag, trigger_rule =trigger_rule)

    failure_fun4 >> task5 


    '''
    case-3
    upstream task --> upstream failed(task7)
    '''
    failure_fun6 = PythonOperator(task_id="failure_fun6", python_callable=failure_fun, dag=dag)
    upstream_failed7 = PythonOperator(task_id="upstream_failed7", python_callable=success_fun, dag=dag)
    task8 = PythonOperator(task_id="task8", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun6 >> upstream_failed7 >> task8


    '''
    case-4
    upstream task --> skipped(task10)
    '''

    skip_task9 = PythonOperator(task_id="skip_task9", python_callable=skip_task, dag=dag)
    task10 = PythonOperator(task_id="task10", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    skip_task9 >> task10

    '''
    case-5
    upstream task --> success(task11,task12)
    '''
    success_fun11 = PythonOperator(task_id="success_fun11", python_callable=success_fun, dag=dag)
    success_fun12 = PythonOperator(task_id="success_fun12", python_callable=success_fun, dag=dag)
    task13 = PythonOperator(task_id="task13", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [success_fun11 , success_fun12] >> task13

    '''
    case-6
    upstream task --> success,failed(task14,task15)
    '''
    failure_fun14 = PythonOperator(task_id="failure_fun14", python_callable=failure_fun, dag=dag)
    success_fun15 = PythonOperator(task_id="success_fun15", python_callable=success_fun, dag=dag)
    task16 = PythonOperator(task_id="task16", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [failure_fun14, success_fun15] >> task16

    '''
    case-7
    upstream task --> failed(task17,task18)
    '''
    failure_fun17 = PythonOperator(task_id="failure_fun17", python_callable=failure_fun, dag=dag)
    failure_fun18 = PythonOperator(task_id="failure_fun18", python_callable=failure_fun, dag=dag)
    task19 = PythonOperator(task_id="task19", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)


    [failure_fun17 , failure_fun18] >> task19

    '''
    case-8 
    upstream task --> skipped(task20,task21)
    '''
    skip_task20 = PythonOperator(task_id="skip_task20", python_callable=skip_task, dag=dag)
    skip_task21 = PythonOperator(task_id="skip_task21", python_callable=skip_task, dag=dag)
    task22 = PythonOperator(task_id="task22", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [skip_task20 , skip_task21] >> task22

    '''
    case-9
    upstream task --> success, skip(task23,task24)
    '''
    success_fun23 = PythonOperator(task_id="success_fun23", python_callable=success_fun, dag=dag)
    skip_task24 = PythonOperator(task_id="skip_task24", python_callable=skip_task, dag=dag)
    task25 = PythonOperator(task_id="task25", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [success_fun23 , skip_task24] >> task25

    '''
    case-10
    upstream task --> failed, skip(task26,task27)
    '''
    failure_fun26 = PythonOperator(task_id="failure_fun26", python_callable=failure_fun, dag=dag)
    skip_task27 = PythonOperator(task_id="skip_task27", python_callable=skip_task, dag=dag)
    task28 = PythonOperator(task_id="task28", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    [failure_fun26 , skip_task27] >> task28

    '''
    case-11
    upstream task --> upstream_failed(task29,task30)
    '''
    failure_fun32= PythonOperator(task_id="failure_fun32", python_callable=failure_fun, dag=dag)
    upstream_failed29 = PythonOperator(task_id="upstream_failed29", python_callable=success_fun, dag=dag)
    upstream_failed30 = PythonOperator(task_id="upstream_failed30", python_callable=success_fun, dag=dag)
    task31 = PythonOperator(task_id="task31", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun32 >> [upstream_failed29 , upstream_failed30] >> task31

    '''
    case-12
    upstream task --> upstream_failed, success(task29,task30)
    '''
    failure_fun33= PythonOperator(task_id="failure_fun33", python_callable=failure_fun, dag=dag)
    success_fn34 = PythonOperator(task_id="success_fn34", python_callable=success_fun, dag=dag, trigger_rule="all_failed")
    upstream_failed35 = PythonOperator(task_id="upstream_failed35", python_callable=success_fun, dag=dag)
    task36 = PythonOperator(task_id="task36", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun33 >> [success_fn34 , upstream_failed35] >> task36


    '''
    case-13
    upstream task --> upstream_failed, failed(task38,task39)
    '''
    failure_fun37= PythonOperator(task_id="failure_fun37", python_callable=failure_fun, dag=dag)
    upstream_failed38 = PythonOperator(task_id="upstream_failed38", python_callable=failure_fun, dag=dag, trigger_rule="all_failed")
    failed_fun39 = PythonOperator(task_id="failed_fun39", python_callable=success_fun, dag=dag)
    task40 = PythonOperator(task_id="task40", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun37 >> [upstream_failed38 , failed_fun39] >> task40

    '''
    case-14
    upstream task --> upstream_failed, skip(task42,task43)
    '''
    failure_fun41= PythonOperator(task_id="failure_fun41", python_callable=failure_fun, dag=dag)
    skip_task42 = PythonOperator(task_id="skip_task42", python_callable=skip_task, dag=dag, trigger_rule="all_failed")
    success_fun43 = PythonOperator(task_id="success_fun43", python_callable=success_fun, dag=dag)
    task44 = PythonOperator(task_id="task44", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun41 >> [skip_task42 , success_fun43] >> task44


    '''
    case-15
    upstream task --> upstream_failed, skip, failed, success(task46,47,48,49)
    '''
    failure_fun45 = PythonOperator(task_id="failure_fun45", python_callable=failure_fun, dag=dag)
    skip_task46 = PythonOperator(task_id="skip_task46", python_callable=skip_task, dag=dag, trigger_rule="all_failed")
    success_fun47 = PythonOperator(task_id="success_fun47", python_callable=success_fun, dag=dag, trigger_rule="all_failed")
    failure_fun48 = PythonOperator(task_id="failure_fun48", python_callable=failure_fun, dag=dag, trigger_rule="all_failed")
    upstream_failed49 = PythonOperator(task_id="upstream_failed49", python_callable=success_fun, dag=dag)
    task50 = PythonOperator(task_id="task50", python_callable=success_fun, dag=dag , trigger_rule =trigger_rule)

    failure_fun45 >> [skip_task46 , success_fun47, failure_fun48, upstream_failed49] >> task50


    return dag


trigger_rules = ["all_success","all_failed","all_done","all_skipped","one_failed","one_success","none_failed","none_failed_min_one_success","none_skipped","always","dummy"]

for trigger_rule in trigger_rules:
    globals()[trigger_rule]=create_dag(trigger_rule)

