from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable



# These args will get passed on to the python operator
default_args = {
    'owner': 'lakshay',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


# define the DAG
dag = DAG(
    'bash_operator_sample',
    default_args=default_args,
    description='python operator',
    schedule_interval='@daily',
)

t1= BashOperator(
    task_id="basic",
    bash_command="whoami",
    dag=dag
)

t2= BashOperator(
    task_id="with_env",
    bash_command="echo Hello $MY_NAME! && echo $A_LARGE_NUMBER",
     env={
            "MY_NAME": "airflow",
            "A_LARGE_NUMBER": "231942"
        },
    dag=dag
)

t3= BashOperator(
    task_id="with_exit_code",
    bash_command='echo "hello world"; exit 99;',
    dag=dag
)

t4= BashOperator(
    task_id="with_custom_exit_code",
    bash_command='echo "hello world"; exit 22;',
    skip_exit_code=22,   
    dag=dag
)

t5= BashOperator(
    task_id="get_execution_date",
    bash_command="echo execution_date = '{{ ds }}' ",
    dag=dag
)

t6 = BashOperator(
    task_id = "get_variable",
    bash_command= 'echo variable filename : "{{ var.value.filename }}"',
    dag=dag
)


t1 >> t2 >> t3 >> t4 >> t5 >> t6