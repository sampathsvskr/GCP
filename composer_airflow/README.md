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

### **[DAG Arguments](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/dag_arguments_and_variables)** to get idea of mostly used arguments.

### **[Operators](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/operators)** 


### **[Trigger Rules](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/trigger_rules)** 

### **[XComs](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/xcoms)** 

### **[Task Groups](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/task_groups_and_branching)** 

### **[Branching](https://github.com/sampathsvskr/GCP/tree/main/composer_airflow/task_groups_and_branching)** 