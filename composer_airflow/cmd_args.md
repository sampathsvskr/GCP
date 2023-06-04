
[Airlflow commands doc](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html)

- ```airflow dags list``` - list the dags
- ```airflow dags delete <DAG_ID>``` - delete the dag
- ```airflow dags show <DAG_ID>``` - Show structure and dependencies of DAG
- ```airflow dags show <DAG_ID> --save <FILE_NAME.png>``` - Save the DAG as file
- ```airflow tasks list <DAG_ID>``` - List the tasks in the dag
- ```airflow dags details <DAG_ID>``` - get dag details
- ```airflow tasks test <DAG_ID> <TASK_ID> <EXECUTION_TIME>``` - Test on a specific task in a DAG.
- ```airflow dags trigger <DAG_ID>``` - Trigger the dag run
- ```airflow dags unpause  <DAG_ID>``` - Unpause the dag
- ```airflow dags pause  <DAG_ID>``` - Pause the dag

## Airflow Database Commands

- ```airflow db init``` initialize a database  
- ```airflow db check``` check the status of your database
- ```airflow db upgrade``` upgrade the information and metadata of your database.
- ```airflow db shell``` access the database from shell