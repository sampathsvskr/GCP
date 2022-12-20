from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'python_operator_sample',
    default_args=default_args,
    description='python operator',
    schedule_interval='@daily',
)

# define python function
def test():
    return "hello world.."

# define the task
t1 = PythonOperator(
        task_id = "basic",
        python_callable= test,
        dag=dag
    )


# define python function
def with_op_args_1(*x):
    print(x)    

# define the task
t2 = PythonOperator(
        task_id = "with_op_args_1",
        python_callable= with_op_args_1,
        op_args=[1,2,3,4],
        dag=dag
    )

# define python function
def with_op_args_2(filename, file_path):
    print(f"file {filename} in {file_path}")    

# define the task
t3 = PythonOperator(
        task_id = "with_op_args_2",
        python_callable= with_op_args_2,
        op_args=["sample.txt","/home/airflow/dags/"],
        dag=dag
    )


# define python function
def with_op_kwargs_1(**x):
    print(x)
    return x

# define the task
t4 = PythonOperator(
        task_id = "with_op_kwargs_1",
        python_callable= with_op_kwargs_1,
        op_kwargs={'operator':'python','service':'airflow'},
        dag=dag
    )

# define python function
def with_op_kwargs_2(operator , service):
    print(operator, service , sep=" , ")
    

# define the task
t5 = PythonOperator(
        task_id = "with_op_kwargs_2",
        python_callable= with_op_kwargs_2,
        op_kwargs={'operator':'python','service':'airflow'},
        dag=dag
    )



# define python function
def with_op_kwargs_3(filename, file_path):
    print(f"file {filename} in {file_path}")    

# define the task
t6 = PythonOperator(
        task_id = "with_op_kwargs_3",
        python_callable= with_op_kwargs_3,
        op_kwargs={"filename" : '{{ var.value.filename }}', "file_path":'{{ var.value.file_path }}'},
        dag=dag
    )


# define python function
def with_op_kwargs_4(filename, file_path):
    print(f"file {filename} in {file_path}")    

# define the task
t7 = PythonOperator(
        task_id = "with_op_kwargs_4",
        python_callable= with_op_kwargs_4,
        op_kwargs=Variable.get("json_variable", deserialize_json=True),
        dag=dag
    )

# define python function
def get_execution_date(ds):
    print(f"Execution date is {ds}")    

# define the task
t8 = PythonOperator(
        task_id = "get_execution_date",
        python_callable= get_execution_date,        
        dag=dag
    )

# define python function
def get_context(**context):
    print(context)    

# define the task
t9 = PythonOperator(
        task_id = "get_context",
        python_callable= get_context,        
        dag=dag
    )

from airflow.operators.python import get_current_context
# define python function
def get_context1():
    print(get_current_context())    

# define the task
t10 = PythonOperator(
        task_id = "get_context1",
        python_callable= get_context1,        
        dag=dag
    )

#decorators
from airflow.decorators import task

@task(dag=dag)
def dec_task(params):
    print(params)


t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> dec_task([1,2,3])

