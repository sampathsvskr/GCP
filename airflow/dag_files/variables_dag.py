from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param



params={
        "x": Param(5, type="integer", minimum=3),
        "y": 6,
        "run_date" : "2022-12-12"
    }  

default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),    
    'email_on_failure': False,
    'email':["test@gmail.com"],
    'email_on_retry': False 
    
}


dag = DAG("variables_test_dag2",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),
           params=params,
           default_args=default_args,
           
           )

# print all the data in context
def get_all_variables_in_context(**context):
    print('Data in the context...')
    for key,value in context.items():
        print(key,value,sep=":")  

task1= PythonOperator(
            task_id="context_data", 
            python_callable=get_all_variables_in_context, 
            dag=dag
            )

# Accessing 'x' defined in params, adding 10 to it and sending it as an argument to pyhton operator
task2 = PythonOperator(
            task_id="var1",
            op_args=[
                "{{ params.x + 10 }}",
            ],
            python_callable=(
                lambda x: print(x)
            ),
            dag=dag
            )




# task_level_params -- change params in task level and printing them
def get_params_in_context(**context):
    print('printing params in context...')
    print(context["params"])
    print(type(context["params"]["x"]))

task3 = PythonOperator(
            task_id="task_level_params",
            params={"x": 10,"z":"hello"},
            python_callable=get_params_in_context,
            dag=dag
         )


# Printing the type of 'y' defined in params
# Even though Params can use a variety of types, the default behavior of templates is to provide your task with a string. 
# You can change this by setting render_template_as_native_obj=True as argument while initializing the DAG.
task4 = PythonOperator(
            task_id="params_type",
            op_args=[
                "{{ params.y }}",
            ],
            python_callable=(
                lambda x: print(type(x))
            ),
         )

# setting 'y' defined param to y_value and sending it as an argument to pyhton operator
y_value= "{{ params.y }}",
date_value= "{{ ds }}"
def print_arguments(y , **context):
    print(y)
    print(context)

task5 = PythonOperator(
            task_id="single_param",
            op_args=[ y_value ],
            python_callable=print_arguments,
            dag=dag
         )

## applying python operations on param values
run_date= "{{ params.run_date.replace('-','') }}",

task6 = PythonOperator(
            task_id="run_date_check",
            op_args=[ run_date ],
            python_callable=print_arguments,
            dag=dag
         )

#op_args with multiple params
def print_arguments(*y , **context):
    print(y)
    print(context)

task7 = PythonOperator(
            task_id="multiple_params",
            op_args=[ y_value,date_value ],
            python_callable=print_arguments,
            dag=dag
         )




task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7

