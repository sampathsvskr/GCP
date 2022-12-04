# **Apache Airflow**

## **XComs**

XComs (short for “cross-communications”) are a mechanism that let Tasks talk to each other, as by default Tasks are entirely isolated and may be running on entirely different machines.
<br><br>
An XCom is identified by a key (essentially its name), as well as the task_id and dag_id it came from. They can have any (serializable) value, but they are only designed for small amounts of data; do not use them to pass around large values, like dataframes.
<br><br>
XComs are explicitly “pushed” and “pulled” to/from their storage using the ```xcom_push``` and ```xcom_pull``` methods on Task Instances. Many operators will auto-push their results into an XCom key called **return_value** if the ```do_xcom_push``` argument is set to True (as it is by default).
<br><br>
```xcom_pull``` defaults to using this(return_value) key if no key is passed to it
<br><br>
XComs are per-task-instance and designed for communication within a DAG run. <br>
We need to use Task Instance object to pull or push this xcoms which will be available in DAG context. 

### References
**[Article by notion on XComs](https://www.notion.so/XComs-3d0fb50edae84342a985ff5ffa266eff)** <br>
**[Simple DAG file with Xcoms](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/xcoms/xcoms_dag.py)**

### Lets see few ways to push and pull xcoms

<br><br>
![alt text](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/images/xcoms.png)
<br><br>

### Returning the value and get it using xcom_pull

```python
def push_xcoms():
    return "Practicing xcoms..."

def pull_xcoms(**context):
    xcom_value= context["ti"].xcom_pull(task_ids="training_models")
    print(xcom_value)


training_models = PythonOperator(
            task_id='training_models',
            python_callable=push_xcoms,
            dag=dag
        )

storing_models = PythonOperator(
            task_id='storing_models',
            python_callable=pull_xcoms,
            dag=dag
        )
```
<br>

- Here task **training_models** which invokes "push_xcoms" is returning some value. This will be stored as XCom.<br>
- ```pull_xcoms``` is reading that value by getting the task instance(ti) from context and using the task_id of the pushed xcom.<br>
- If we see in the above image, for task_id **training_models**, key is taken as ```return_value``` as we just returned it.
<br><br>

### Use xcom_push and xcom_pull

```python
## Pushing xcom 

def push_xcom_with_push_xcoms_with_ti(ti):
    models = ['Logistic', 'Decision Tree', 'XGBoost']
    ti.xcom_push(key='ml_models', value=models)


def pull_xcom_with_pull_xcoms_with_ti(ti):
    models= ti.xcom_pull(task_ids="push_xcom_with_push_xcoms_with_ti",key='ml_models')
    print(models)


push_xcom_with_push_xcoms_with_ti_task = PythonOperator(
            task_id='push_xcom_with_push_xcoms_with_ti',
            python_callable=push_xcom_with_push_xcoms_with_ti,
            dag=dag
        )

pull_xcom_with_pull_xcoms_with_ti_task = PythonOperator(
            task_id='pull_xcom_with_pull_xcoms_with_ti',
            python_callable=pull_xcom_with_pull_xcoms_with_ti,
            dag=dag
        )
```
<br>

- Here we have passed ``ti`` directly as parameter to python. Airflow interprets it as Task Instance. Another way to get Task Instance.<br>
- Pushing the XCom using ```xcom_push``` with key to it.<br>
- Then reading the pushed XComs using ```pull_xcoms``` with the same key and task name<br>
- If we see in the above image, for task_id **push_xcom_with_push_xcoms_with_ti_task**, has key as mentioned while push_xcom.

<br><br>

### Fetching xcoms from multiple tasks

```python

def pull_xcom_from_multiple_tasks(**context):

    models= context["ti"].xcom_pull(task_ids=["push_xcom_with_push_xcoms","push_xcom_with_push_xcoms_with_ti"],key="ml_models")
    print(models)


pull_xcom_from_multiple_tasks_task = PythonOperator(
            task_id='pull_xcom_from_multiple_tasks',
            python_callable=pull_xcom_from_multiple_tasks,
            dag=dag
        )
```

<br>

- If you look into the ```task_ids``` param in ```xcom_pull```, it is plural.
- SO, we can fetch XComs from multiple tasks but they should have same key.


<br><br>

### Pulling Xcoms using jinja templates

```{{ ti.xcom_pull(task_ids=<task_ids>,key=<key>) }}```

```python
def pull_xcom_from_templates(**op_kwargs):
    print(op_kwargs) 
    print(op_kwargs["models"])

# Fetching the XCom using template and passing it as an argument to python callable fn.
pull_xcom_from_templates_task = PythonOperator(
    task_id="pull_xcom_from_templates",
    python_callable = pull_xcom_from_templates,
    op_kwargs = {
        'models' : '{{ ti.xcom_pull(task_ids="push_xcom_with_push_xcoms",key="ml_models") }}'
    },
    dag=dag

) 

# Push XCom using bash operator
push_xcom_from_templates_task_bash =  BashOperator(
    task_id = "push_xcom_from_templates_task_bash",
    bash_command='echo "From Bash ->  {{ ds }}"',    
    dag=dag
)

# Push XCom using bash operator - jinja template
pull_xcom_from_templates_task_bash =  BashOperator(
    task_id = "pull_xcom_from_templates_task_bash",
    bash_command='echo From Bash ->  {{ ti.xcom_pull(task_ids="push_xcom_from_templates_task_bash") }}',
    dag=dag
     
)
```

<br>

- Another way to fecth XComs is using jinja template.
- Make spacing is same as like mentioned inside the template i.e inside curly braces.