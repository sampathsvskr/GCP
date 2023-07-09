## Google Cloud Storage

## Python client

Install library
```sh
pip install google-cloud-storage
```

## Importing storage client
```python
from google.cloud import storage
project_id = 'qwiklabs-gcp-00-73bbdba69620'
client = storage.Client(project=project_id)
```

## Storage client from service account
```python
client = storage.Client.from_service_account_json('/google-cloud/keyfile/service_account.json')
```

## Create a bucket in GCS
```python
bucket = client.bucket('mybucket')
bucket.location = 'eu'
bucket.create()
```


## List blobs in bucket
```python
bucket=client.get_bucket("bucket_name")
blobs=bucket.list_blobs(prefix="test_", delimiter="/")
for blob in blobs:
    print(blob)
```

## Download file to local from GCS
```python
bucket=client.get_bucket("bucket_name")
blobs=bucket.list_blobs(prefix="test_", delimiter="/")
for blob in blobs:
     for blob in blobs: 
    destination_uri = f'test/{blob}'
    blob.download_to_filename(destination_uri)
```


## Copy file from local to GCS bucket
```python
# assign to which bucket we need to push
bucket = client.bucket("<bucket>")

# file location in GCS
blob =  bucket.blob("test/sample.py")

# upload file to GCS
blob.upload_from_filename("<local_file_path>")
```

## Copy folders from local to GCS bucket
```python
import os
# assign to which bucket we need to push
bucket = client.bucket("<bucket>")

local_path = "<folder_path>"

for root, dirs, files in os.walk(local_path):
    for file_name in files:
        local_file_path = os.path.join(root, file_name)
        blob =  bucket.blob(local_file_path)
        blob.upload_from_filename(local_file_path)
```

## Delete file from GCS
```python

# assign to which bucket we need to push
bucket = client.bucket("<bucket>")

# file location in GCS
blob =  bucket.blob("test/sample.py")

# upload file to GCS
blob.delete()

```

## Zip Files

https://dev.to/jakewitcher/uploading-and-downloading-zip-files-in-gcp-cloud-storage-using-python-2l1b