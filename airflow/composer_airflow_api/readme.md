## Airflow API 
## [documentaion](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#tag/DAG)

- For GCP to access composer airflow, need service account details.
- If GCP cannot fetch then by default, we need to get the service account key json and set the path of that json file to GOOGLE_APPLICATION_CREDENTIALS as env variable.

### Steps to set service_account_key path:
    ### Linux
    export GOOGLE_APPLICATION_CREDENTIALS=<path_of_service_account_key>

    ### Windows
    set GOOGLE_APPLICATION_CREDENTIALS=<path_of_service_account_key>

    ### Directly set in py file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=<path_of_service_account_key>
