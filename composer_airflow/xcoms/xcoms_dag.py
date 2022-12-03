'''
Different ways to pull and push Xcoms

'''

from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.param import Param


default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,
    'email_on_failure': False,
    'email':["test@gmail.com"],
    'email_on_retry': False 
    
}


dag = DAG("xcom_test_dag",
           #schedule_interval=None,
           catchup=True,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )

#********************************************************************
## Return keyword

def push_xcoms():
    return "Practicing xcoms..."

def pull_xcoms(**context):
    xcom_value= context["ti"].pull_xcom(task_ids="training_models")
    print(xcom_value)


training_models = PythonOperator(
            task_id='training_models',
            python_callable=push_xcoms
        )

storing_models = PythonOperator(
            task_id='storing_models',
            python_callable=pull_xcoms
        )

training_models >> storing_models
#********************************************************************

## Pushing xcom 

def push_xcom_with_push_xcoms_with_ti(ti):
    models = ['Logistic', 'Decision Tree', 'XGBoost']
    ti.xcom_push(key='ml_models', value=models)


def pull_xcom_with_pull_xcoms_with_ti(ti):
    models= ti.xcom_pull(task_ids="push_xcom_with_push_xcoms_with_ti",key='ml_models')
    print(models)


push_xcom_with_push_xcoms_with_ti_task = PythonOperator(
            task_id='push_xcom_with_push_xcoms_with_ti',
            python_callable=push_xcom_with_push_xcoms_with_ti
        )

pull_xcom_with_pull_xcoms_with_ti_task = PythonOperator(
            task_id='pull_xcom_with_pull_xcoms_with_ti',
            python_callable=pull_xcom_with_pull_xcoms_with_ti
        )

storing_models >> push_xcom_with_push_xcoms_with_ti_task >> pull_xcom_with_pull_xcoms_with_ti_task
#********************************************************************

## Pushing xcom 

def push_xcom_with_push_xcoms(**context):
    models = ['SVM', 'Random Forest', 'KNN']
    context['ti'].xcom_push(key='ml_models', value=models)


def pull_xcom_with_pull_xcoms(**context):
    models= context['ti'].xcom_pull(task_ids="push_xcom_with_push_xcoms",key='ml_models')
    print(models)

push_xcom_with_push_xcoms_task = PythonOperator(
            task_id='push_xcom_with_push_xcoms',
            python_callable=push_xcom_with_push_xcoms
        )

pull_xcom_with_pull_xcoms_task = PythonOperator(
            task_id='pull_xcom_with_pull_xcoms',
            python_callable=pull_xcom_with_pull_xcoms
        )

pull_xcom_with_pull_xcoms_with_ti_task >> push_xcom_with_push_xcoms_task >>  pull_xcom_with_pull_xcoms_task
#********************************************************************

## Include prior dates xcoms
from airflow.models import XCom

def pull_xcom_for_prior_dates(**context):
    models= context["ti"].xcom_pull(task_ids="push_xcom_with_push_xcoms",key="ml_models",include_prior_dates=True)
    print(models)

    models = XCom.get_many(
                execution_date = context["execution_date"],
                dag_ids= context["dag"].dag_id,
                include_prior_dates= True
                )
    print(models)

pull_xcom_for_prior_dates_task = PythonOperator(
            task_id='pull_xcom_for_prior_dates',
            python_callable=pull_xcom_for_prior_dates
        )

pull_xcom_with_pull_xcoms_task >>  pull_xcom_for_prior_dates_task
#********************************************************************

## Fetching xcoms from multiple tasks

def pull_xcom_from_multiple_tasks(**context):

    models= context["ti"].xcom_pull(task_ids=["push_xcom_with_push_xcoms","push_xcom_with_push_xcoms_with_ti"],key="ml_models")
    print(models)


pull_xcom_from_multiple_tasks_task = PythonOperator(
            task_id='pull_xcom_from_multiple_tasks',
            python_callable=pull_xcom_from_multiple_tasks
        )

pull_xcom_for_prior_dates_task >> pull_xcom_from_multiple_tasks_task
#********************************************************************

## Pulling Xcoms using templates

def pull_xcom_from_templates(**op_kwargs):
    print(op_kwargs) 
    print(op_kwargs["models"])

pull_xcom_from_templates_task = PythonOperator(
    task_id="pull_xcom_from_templates",
    python_callable = pull_xcom_from_templates,
    op_kwargs = {
        'models' : '{{ ti.xcom_pull(task_ids="push_xcom_with_push_xcoms",key="ml_models") }}'
    }

) 

push_xcom_from_templates_task_bash =  BashOperator(
    task_id = "push_xcom_from_templates_task_bash",
    bash_command='echo "From Bash ->  {{ ds }}"',
    xcom_push=True 
)

pull_xcom_from_templates_task_bash =  BashOperator(
    task_id = "pull_xcom_from_templates_task_bash",
    bash_command='echo From Bash ->  {{ ti.pull_xcom(task_ids="push_xcom_from_templates_task_bash" }}'
     
)

pull_xcom_from_multiple_tasks_task >> pull_xcom_from_templates_task >> push_xcom_from_templates_task_bash >> pull_xcom_from_templates_task_bash

   
