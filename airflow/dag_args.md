# **Apache Airflow**

## **Dag Arguments**

### Mostly used args


- The default args will be passed to each operator
- You can override them on a per-task basis during operator initialization

<br>

```python
default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),    
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 6, 1)
    
}

with DAG(dag_id = 'sample_dag',
          schedule_interval='@daily',  # Cron expression, here it is a preset of Airflow, @daily means once 
          default_args=default_args,
          description ='sample tutoruial',
          concurrency = 2,
          max_active_runs=1,
          catchup= False,
          dagrun_timeout = timedelta(minutes=60),
          tags = ['testing'] ,
          # report failure is a method which triggers smtp mail
          on_failure_callback = report_failure
            ) as dag:


```
<br>

### owner 
- represents owner of dag. 
- If onwer="", it takes "airflow" as owner

### depends_on_past
- Task can only run if the previous run of the task in the previous DAG Run succeeded
-  https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#concepts-depends-on-past

### start_date
- From when dag needs to be started
```pyhton
start_date: datetime(2022, 1, 1)
schedule_interval: '@daily'
```
- The first run will kick in at 2022-01-02 at 00:00, this run execution_date will be: 2022-01-01 00:00

### email_on_failure
- Send an email when the task is failed
- Configure smtp with mailids, subject and body for sending email.

### email_on_retry
- Send an email when the task is failed and trying to re-execute
- Configure smtp with mailids, subject and body for sending email.

### retries
- No. of time needs to tried before updating task as failed.

### retry_delay
- How much time need to wait between every retry

### end_date
- When to end the execution of dag
- Default is run forever

### concurrency
- No. of tasks to be executed in parallel

### schedule_interval
- Accepts cron expression and below string formats.

|preset	    |meaning	                                                     |  cron     |
| ----------|:------------------------------------------------------------:| ---------:|
|None	    |Don’t schedule, use for exclusively “externally triggered” DAGs |	         |
|@once	    |Schedule once and only once	                                 |           |
|@hourly	|Run once an hour at the beginning of the hour	                 | 0 * * * * |
|@daily	    |Run once a day at midnight	                                   | 0 0 * * * |
|@weekly	|Run once a week at midnight on Sunday morning	                 | 0 0 * * 0 |
|@monthly   |Run once a month at midnight of the first day of the month	   | 0 0 1 * * |
|@yearly	|Run once a year at midnight of January 1	                       | 0 0 1 1 * |


## catchup
- Airflow allows missed DAG Runs to be scheduled again so that the pipelines catchup on the schedules that were missed for some reason
- https://medium.com/nerd-for-tech/airflow-catchup-backfill-demystified-355def1b6f92

## max_active_runs
- Max no. of instances of certain dag to be run i.e. If an istance of a dag is running and you trigger the same dag again, the new instance starts once the already running instance is completed.

## dagrun_timeout
- How long a DagRun should be up before timing out / failing, so that new DagRuns can be created.

## on_failure_callback
- If any task is failed, call the function which is mentioned..
- Basically we use to trigger email as task failure acknowledgment

## for more dag args, [airflow documentaion](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG)


<br><br><br>


## **Dag Variables**

### **[Airflow default variables doc](https://airflow.apache.org/docs/apache-airflow/1.10.5/macros.html#default-variables)**
Example DAG py file playing with params
**[variables_dag.py](https://github.com/sampathsvskr/GCP/blob/main/airflow/dag_files/variables_dag.py)**
- If we need to access these variables in any operator, we can access from context and if from the dag itself not in operators then use same format as in doc
- Even though Params can use a variety of types, the default behavior of templates is to provide your task with a string. 
- You can change this by setting **render_template_as_native_obj=True** as argument while initializing the DAG.

```python
from airflow import DAG
from airflow.models.param import Param

def print_x(**context):    
    # extracting user defined params
    print(context["params"]["x"])
    
    # execution date
    print(context["ds"])

the_dag= DAG(
    "sample_dag",
    params={
        "x": Param(5, type="integer", minimum=3),
        "y": 6
    },
) 

# make sure there is space between parenthesis and param
# accessing outside operators and inside dag
y_value= "{{ params.y }}"

PythonOperator(
    task_id="print_x",
    python_callable=print_it,
    dag=the_dag
)
```









