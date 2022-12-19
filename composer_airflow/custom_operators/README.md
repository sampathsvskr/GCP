# **Apache Airflow**

## **Sample Custom Operator**

Created a custom custom operator with reference to BigQueryInsertJobOperator.<br><br>
What does this custom operator do? <br>
- It takes the sql file or query which contains params as string and param values as dict 
- Replaces the params with values in query 
- Executes the query in BigQuery

**[Custom operator code](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/custom_operators/custom_operator.py)**

<br>In the main dag file, we can use this custom operator similarly as normal operator

**[DAG file](https://github.com/sampathsvskr/GCP/blob/main/composer_airflow/custom_operators/dag_file.py)**