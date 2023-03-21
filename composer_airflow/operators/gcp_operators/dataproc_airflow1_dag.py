import os
from datetime import datetime
from datetime import timedelta

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator

CLUSTER_CONFIG = ClusterGenerator(
    project_id=models.Variable.get('gcp_project'),
    region="us-central1",
    cluster_name="test-airflow-job",
    tags=["dataproc"],
    storage_bucket='bc1234',
    num_workers=2,
    num_masters=1,
    master_machine_type="n1-standard-4",
    master_disk_type="pd-standard",
    master_disk_size=512,
    worker_machine_type="n1-standard-4",
    worker_disk_type="pd-standard",
    worker_disk_size=512,
    properties={},
    image_version="2.0",
    autoscaling_policy=None,
    idle_delete_ttl=7200,
    #optional_components=['JUPYTER', 'ANACONDA'],
    metadata={"gcs-connector-version" : '2.1.1' , 
                  "bigquery-connector-version": '1.1.1',
                  "spark-bigquery-connector-version": '0.17.2',
                  "PIP_PACKAGES" : 'tensorflow'
                 },
    init_actions_uris =['gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh','gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh']
).make()

PYSPARK_URI='gs://bc1234/big_query_spark.py'

PYSPARK_JOB = {
    "reference": {"project_id": models.Variable.get('gcp_project')},
    "placement": {"cluster_name": "test-airflow-job"},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,"jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"]},
}


with models.DAG(
    "example_gcp_dataproc",    
    start_date=datetime(2022, 8, 15),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_test_dataproc_cluster",
        cluster_name="test-airflow-job",
        project_id=models.Variable.get('gcp_project'),
        region="us-central1",
        cluster_config=CLUSTER_CONFIG,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        location='us-central1', 
        project_id=models.Variable.get('gcp_project')
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=models.Variable.get('gcp_project'), 
        cluster_name='test-airflow-job', 
        region='us-central1'
    )
    
    
    create_dataproc_cluster >> submit_job >> delete_cluster