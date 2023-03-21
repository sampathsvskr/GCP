
rom datetime import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
            DataprocCreateClusterOperator,
            DataprocDeleteClusterOperator,
            DataprocSubmitJobOperator
            
             )
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator


project_id="<project_id>"
region="<region>"
gcp_conn_id = "<connection>" # needed to connect to your project, if airflow is not running in same project

# creating the cluster config
CLUSTER_CONFIG = {
    # master config details
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    # worker details
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    # Actions to be performed while creating cluster
    "initialization_actions":[
        {
            "executable_file":"gs://bucket1/init.sh" ## we can run requirements.txt from here
        }
    ],
    # cluster metadata
    "gce_cluster_config":{
        "metadata":{
            "name":"test",
            "PIP_PACKAGES":'pandas-gbq scikit-learn', # package to install
            "spark-bigquery-connector-version":'0.21.0',
            "bigquery-connector-version":"1.2.0"
            }
    },
    #software configs
    "software_config":{
        "properties":{
            'dataproc:dataproc.logging.stackdriver.job.driver.enable':'true',
            'dataproc:dataproc.logging.stackdriver.job.yarn.container.enable':'true',
            'dataproc:dataproc.logging.stackdriver.enable':'true',
            'dataproc:jobs.file-backed-output.enable':'true',
            'dataproc:dataproc.monitoring.stackdriver.enable':'true'
        },
        'optional_components':['HIBE_WEBHCAT','ZOOKEEPER','DCOKER','JUPYTER'],
        'image_version':"2.0-rocky8"    
    }
}

'''
If we need to create single node cluster:
- worker_config is not necessary 
- add "dataproc:dataproc.allow.zero.properties":"true" under properties of software_config

If we need autoscaling cluster:
- remove worker_config as it takes from autoscaling policy
- add CLUSTER_CONFIG['autoscaling_config]={"policy_uri":projects/<project_id>/regions/<region>/autoscalingPolicies/<policy_name>}
- dataproc region and policy region should be same
'''


dag = DAG("bigquery_operators",
           #schedule_interval=None,
           catchup=False,
           start_date=dates.days_ago(2),           
           schedule_interval= None
                      
           )

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id=project_id,
    cluster_config=CLUSTER_CONFIG,
    region=region,
    cluster_name="test-cluster",
    gcp_conn_id=gcp_conn_id, # service account connection
    labels=['test_cluster'],
    dag=dag
)

PYSPARK_JOB = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": "test-cluster"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://<bucket>/<pyspark_file>",
        "args":"<args>"
        },
}

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PYSPARK_JOB, 
    region=region,
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

PIG_JOB = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": "test-cluster"},
    "pig_job": {
        "query_list": {"queries":["sh gsutil cp gs://bucket/test.sh /tmp; sh chmod +x /tmp/test.sh"]}
        },
}

pig_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PIG_JOB, 
    region=region,
    project_id=project_id,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=project_id,
    cluster_name="test-cluster",
    region=region,
    gcp_conn_id=gcp_conn_id,
    dag=dag
)

def create_touch_file(run_date, **context):
    gcshook= GCSHook(gcp_conn_id=gcp_conn_id)
    gcshook.upload(
        bucket_name = "<bucket_name>",
        object_name = "test.done",
        filename=None #local path to the file
        data=f'run_date:{run_date}',
        encoting='utf-8'
    )
create_touch_file_task= PythonOperator(
        task_id = "create_file",
        python_callable= create_touch_file,    
        provide_context=True,
        op_kwargs = {
            "run_date":'{{ execution_date }}'
        }
        dag=dag
    )

create_cluster >> pyspark_task >> pig_task >> delete_cluster >> create_touch_file_task
