## Airflow API 
## [documentaion](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#tag/DAG)

- For composer to access airflow, need auth creditionals, if it cannot be fetched by default, we need to set the path of service_account_key json file to GOOGLE_APPLICATION_CREDENTIALS as env variable.

### Steps to set service_account_key path:
    ### Linux
    export GOOGLE_APPLICATION_CREDENTIALS=<path_of_service_account_key>

    ### Windows
    set GOOGLE_APPLICATION_CREDENTIALS=<path_of_service_account_key>

    ### Directly set in py file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=<path_of_service_account_key>
