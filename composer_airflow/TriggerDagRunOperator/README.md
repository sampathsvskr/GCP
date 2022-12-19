# **Apache Airflow**

## **TriggerDagRunOperator**

Why TriggerDagRunOperator ?<br>
It helps to trigger another DAG from the current DAG and wait for its completion before executing the next task of current DAG. 

```python
trigger_dag_1 = TriggerDagRunOperator(
        task_id='trigger_sub_dag', 
        trigger_dag_id= "sub_dag",
        execution_date = '{{ ds }}' , 
        reset_dag_run = True, 
        wait_for_completion = True, 
        poke_interval = 10 
    )
```

- `trigger_dag_id` - id of the dag, which needs to be triggered
- `execution_date` - to share same execution date between dags. Might be usefull to share data which is execution date dependent or XComs
- `reset_dag_run` - to execute DAGRun multiple times on the same execution dates and for backfilling 
- `wait_for_completion` - wait for its completion before executing the next task
- `poke_interval` - Poke interval to check dag run status when wait_for_completion=True. (default: 60)
- `conf` - configuration for the dag

### References
**[Aiflow doc](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/trigger_dagrun/index.html)** <br>
**[Artile from notion.io](https://www.notion.so/The-TriggerDagRunOperator-699b5483f34a42e495183fd028b68267)**
