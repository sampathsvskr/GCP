# Apache Airflow

## Basics of DAG
DAG or a Directed Acyclic Graph – is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies.<br> 
DAGs are defined in standard Python files that are placed in Airflow’s DAG_FOLDER. <br>
Airflow will execute the code in each file to dynamically build the DAG objects.<br>
You can have as many DAGs as you want, each describing an arbitrary number of tasks. <br>
In general, each one should correspond to a single logical workflow.<br><br>

Main components of a DAG file
 - Defining the DAG
 - Defining the tasks
 - Task dependencies

 ### **Try this basic [Hello world dag](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/dag_arguments_and_variables/hello_world_dag.py)**

 <br>

## Defining the DAG
There are three ways to declare a DAG. 
- Context manager, which will add the DAG to anything inside it implicitly:
```python
with DAG("my_dag_name") as dag:
    op = DummyOperator(task_id="task")
```

- Standard constructor, passing the dag into any operators you use:
```python
my_dag = DAG("my_dag_name")
op = DummyOperator(task_id="task", dag=my_dag)
```
- @dag decorator to turn a function into a DAG generator:

```python
@dag(start_date=days_ago(2))
def generate_dag():
    op = DummyOperator(task_id="task")

dag = generate_dag()
```
<br>

To get idea of mostly used arguments => **[DAG Arguments](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/dag_arguments_and_variables)** 


**[Operators](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/operators)** 


Set dependencies between tasks => **[Trigger Rules](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/trigger_rules)** 

To share data/values between tasks => **[XComs](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/xcoms)** 

SubDags is deprecated hence TaskGroup is the preferred choice.
Collection of tasks as one group => **[Task Groups](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/task_groups_and_branching)** 

Choosing tasks based on conditions => **[Branching](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/task_groups_and_branching#branching)** 

Task callbacks to act upon changes of state => **[call_backs](https://airflow.apache.org/docs/apache-airflow/2.2.2/logging-monitoring/callbacks.html)**

Trigger a DAG from another DAG => **[TriggerDagRunOperator](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/TriggerDagRunOperator)**


Decorators => **[Decorators](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/decorators)**


Custom operators => **[Custom operator](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/custom_operators)**
