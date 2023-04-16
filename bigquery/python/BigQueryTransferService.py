## Transfer dataset from one project to other project using Big Query Transfer Service

'''
Libraries to be installed: 
    pip install --upgrade google-cloud-bigquery-datatransfer
'''

from google.cloud import bigquery_datatransfer

client = bigquery_datatransfer.DataTransferServiceClient()
dest_project_id = "dest_demo_project"
dest_dataset_id = "dest_demo_dataset"
source_project_id = "source_project"
source_dataset = "source_dataset"

transfer_config = bigquery_datatransfer.TransferConfig(
    destination_dataset_id = dest_dataset_id,
    display_name = "copy_data",
    data_source_id = "cross_region_copy",
    params={
    "source_project_id":source_project_id,
    "source_dataset_id":source_dataset,
    },
    schedule = "every 24 hours",

)

transfer_config = client.create_tansfer_config(
    parnet = client.common_project_path(dest_project_id),
    transfer_config=transfer_config
)

print("Transfer config created")