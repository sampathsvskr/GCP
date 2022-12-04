from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Alreday set from Airflow UI
var1 = Variable.get("var1")

def set_variables():

    # set variables
    Variable.set(key="var2",value="airflow")
    Variable.set(key="json_var",value={"email":"test@gmail.com"})

    print("JSON data before deserialization")
    json_data = Variable.get("json_var")
    print(json_data)
    print(type(json_data))

    print("JSON data after deserialization")
    json_data = Variable.get("json_var",deserialize_json=True)
    print(json_data)
    print(type(json_data))




default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,    
    'email_on_failure': False,
    'email':["test@gmail.com"],
    'email_on_retry': False 
    
}


dag = DAG("default_args_test_dag2",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )


python_operator = PythonOperator(
    task_id = "set_variables",
    python_callable= set_variables,
    dag= dag
)

# get the variables using jinja template
bash_operator1 = BashOperator(
    task_id = "get_variables1",
    bash_command= 'echo var1 : "{{ var.value.var1 }}"',
    dag=dag
)

bash_operator2 = BashOperator(
    task_id = "get_variables2",
    bash_command= 'echo var2 : "{{ var.value.var2 }}"',
    dag=dag
)

python_operator >> bash_operator1 >> bash_operator2
