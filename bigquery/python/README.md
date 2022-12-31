
## sample pandas data frame
```python
import pandas
import datetime

records = [
    {"transaction_id": 1, "transaction_date": datetime.date(2021, 10, 21)},
    {"transaction_id": 2, "transaction_date": datetime.date(2021, 10, 21)},
    {"transaction_id": 3, "transaction_date": datetime.date(2021, 10, 21)},
    {"transaction_id": 1, "transaction_date": datetime.date(2021, 10, 24)},
    {"transaction_id": 2, "transaction_date": datetime.date(2021, 10, 24)},
    {"transaction_id": 3, "transaction_date": datetime.date(2021, 10, 24)}
]

df = pandas.DataFrame(records)
```

## Python client

Install library
```sh
pip install google-cloud-bigquery
```

Import bigquery and create client 
```python
from google.cloud import bigquery
project_id = 'qwiklabs-gcp-00-73bbdba69620'
client = bigquery.Client(project=project_id)
```

Run a query
```python
query_job = client.query(
    """
    SELECT
      id,name
    FROM `myProject.myDataset.myTable`
    LIMIT 10"""
)

results = query_job.result()  # Waits for job to complete.
for row in results:
    print("{} : {} views".format(row[0], row[1]))
```

## Writing data to bigquery

```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"    
)

table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) 
job.result() 
```

Date partitioning
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    
    # This is needed if table doesn't exist, but won't hurt otherwise:
    time_partitioning=bigquery.table.TimePartitioning(type_="DAY",field="transaction_date")
)

# Include target partition in the table id:
table_id = f"{project_id}.{dataset}.{table}s" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result() # Wait for job to finish
```
