#https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html
#https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/bigquery.html

from datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import (
            BigQueryCreateEmptyDatasetOperator,
            BigQueryCreateEmptyTableOperator,
            BigQueryGetDatasetOperator,
            BigQueryGetDatasetTablesOperator,
            BigQueryInsertJobOperator,
            BigQueryCheckOperator,
            BigQueryValueCheckOperator
             )


default_args= {
    
    'owner': 'myself',
    'depends_on_past': False,
    'email_on_failure': False,
    'email':["test@gmail.com"],
    'email_on_retry': False 
    
}

project_id="test"
gcp_conn_id = "gcp_conn_id"


dag = DAG("bigquery_operators",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),           
           default_args=default_args
                      
           )


create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='Org',
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,    
    dag=dag)


get_dataset = BigQueryGetDatasetOperator(
    dataset_id = "Org", 
    project_id=project_id, 
    gcp_conn_id=gcp_conn_id, 
    dag = dag)


schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
               {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
               {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

createTable = BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    dataset_id='Org',
    table_id='employees',
    project_id=project_id,
    gcs_schema_object='gs://schema-bucket/employee_schema.json',
    gcp_conn_id=gcp_conn_id,
    #google_cloud_storage_conn_id='airflow-conn-id',
    dag=dag
)

schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
               {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
               {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
               {"name": "join_date", "type": "DATE", "mode": "REQUIRED"}]

createPartitionedTable = BigQueryCreateEmptyTableOperator(
    task_id='create_partitioned_table',
    dataset_id='Org',
    table_id='employees_partitioned',
    project_id=project_id,
    gcs_schema_object='gs://schema-bucket/employee_schema.json',
    gcp_conn_id=gcp_conn_id,
    time_partitioning = {
                        "type": "DAY",                        
                        "field": "join_date"
                        },
    #google_cloud_storage_conn_id='airflow-conn-id',
    dag=dag
)

get_dataset_tables = BigQueryGetDatasetTablesOperator(
    task_id="get_dataset_tables", 
    dataset_id='Org',
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

query = f'''
INSERT INTO `{project_id}.org.employees`(id, emp_name, salary) VALUES (1, 'sam', 1200000) , (2,'john',600000);
INSERT INTO `{project_id}.org.employees_partitioned`(id, emp_name, salary,join_date) 
VALUES (1, 'sam', 1200000 , "2022-10-10") , (2,'john',600000, "2022-10-10"), (3,'revi',600000, "2020-01-11")
'''
insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag
)


'''
This operator expects a sql query that will return a single row. 
Each value on that first row is evaluated using python bool casting. 
If any of the values return False the check is failed and errors out.
'''

check_count = BigQueryCheckOperator(
    task_id="check_count",
    sql=f"SELECT COUNT(*) FROM `Org.employees`",
    use_legacy_sql=False,
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

'''
These operators expects a sql query that will return a single row. 
Each value on that first row is evaluated against pass_value which can be either a string or numeric value
'''
check_value = BigQueryValueCheckOperator(
    task_id="check_value",
    sql=f"SELECT COUNT(*) FROM `Org.employees_partitioned`",
    pass_value=3,
    use_legacy_sql=False,
     project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

query = f'''
DELETE FROM `{project_id}.org.employees` WHERE id=2; 
'''
delete_query_job = BigQueryInsertJobOperator(
    task_id="delete_query_job",
    configuration={
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id=gcp_conn_id,
    dag=dag
)


create_dataset >> get_dataset >> createTable >> createPartitionedTable >> get_dataset_tables >> insert_query_job >> check_count >> check_value >> delete_query_job
    
