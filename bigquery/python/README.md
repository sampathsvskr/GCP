
## Referal links
https://gcloud.readthedocs.io/en/latest/bigquery-client.html
https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJobConfig
https://www.programcreek.com/python/example/115622/google.cloud.bigquery.QueryJobConfig
https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/notebooks/rendered/bigquery-basics.md
https://github.com/GoogleCloudDataproc/spark-bigquery-connector <br>
https://medium.com/google-cloud/apache-spark-bigquery-connector-optimization-tips-example-jupyter-notebooks-f17fd8476309



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

## Create bigquery client using service account
```python
from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file('path/to/file.json')

project_id = 'my-bq'
client = bigquery.Client(credentials= credentials,project=project_id)
```

## Create dataset
```python
from google.cloud import bigquery
client = bigquery.Client()

dataset_id = f"{client.project}.demo_dataset"
dataset = bigquery.Dataset(dataset_id)
dataset.location = 'us-east4'

#raises exception, if dataset already available
dataset = client.create_dataset(dataset,timeout=30)

print(f"Dataset created : {client.project}.{dataset.dataset_id}")
```

## Create table
```python
from google.cloud import bigquery
client = bigquery.Client()

table_id = f"{client.project}.demo_dataset.demo_table"

schema = [
  bigquery.SchemaField("id","INTEGER",mode= "REQUIRED"),
  bigquery.SchemaField("name","STRING",mode= "NULLABLE"),
  bigquery.SchemaField("location","STRING",mode= "NULLABLE"),
  bigquery.SchemaField("data","FLOAT",mode= "NULLABLE"),
  
]

table = bigquery.Table(table_id,schema = schema)
table = client.create_table(table)

print(f"Table created : {table.project}.{table.dataset_id}.{table.table_id}")

```
## Create external table
```python
from google.cloud import bigquery

client = bigquery.Client()
#Define your schema
schema = [
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]



#dataset_ref = client.dataset('<your-dataset>')
#table_ref = bigquery.TableReference(dataset_ref, '<your-table-name>')
#table = bigquery.Table(table_ref, [schemafield_col1,schemafield_col2])

# configs to external table
dataset_ref = bigquery.DatasetReference('<project>','<dataset>')
table = bigquery.Table(dataset_ref.table('<table_name>'),schema=schema)

external_config = bigquery.ExternalConfig('CSV')
# source file uri
source_uris = ['<url-to-your-external-source>'] #i.e for a csv file in a Cloud Storage bucket 
                                              #it would be something like "gs://<your-bucket>/<your-csv-file>"
external_config.source_uris = source_uris
external_config.options.skip_leading_rows=1
table.external_data_configuration = external_config

# create table
table = client.create_table(table)

#query table
query_job = client.query(f'select * from {dataset_id}.{table_id}')
results = query_job.result()
for row in results:
  print(row)

```

## Run a query
```python
query_job = client.query(
    """
    SELECT
      id,name
    FROM `myProject.myDataset.myTable`
    LIMIT 10"""
)

results = query_job.result()  # wait for the job to complete
for row in results:
    print("{} : {} views".format(row[0], row[1]))

df = query_job.to_dataframe()
```

## Run a parameterized query
```python

query = """
SELECT 
country_name,
EXTRACT(month FROM date) as month,
EXTRACT(year FROM date) as year,
SUM(cumulative_confirmed) as cum_sum_cases
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE country_code=@country and date < @date
GROUP BY 1,2,3
ORDER BY 1,2,3;
"""
# job config
job_config = bigquery.QueryJobConfig(
  #query params
  query_parameters=[
    bigquery.ScalarQueryParameter('country','STRING','IN'),
    bigquery.ScalarQueryParameter('date','DATE','2021-10-10'),
  ]
)
query_job = client.query(
  query,
  job_config=job_config    
)

results = query_job.result()  # wait for the job to complete
for row in results:
    print("{} : {} views".format(row[0], row[1]))

df = query_job.to_dataframe()
```



## Writing data to bigquery

```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"    
)

table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) 
job.result()  # wait for the job to complete 
```

## Writing data from csv to bigquery

```python
job_config = bigquery.LoadJobConfig(
   skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",
    source_format=biguery.SourceFormat.CSV,    
)
uri = "gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_2.csv"

table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_uri(uri, table_id, job_config=job_config) 
job.result()  # wait for the job to complete  

```

## Writing nested json data from GCS to bigquery

```python

schema = [
  bigquery.SchemaField("PlayerName","STRING",mode= "NULLABLE"),
  bigquery.SchemaField("Age","INTEGER",mode= "NULLABLE"),
  bigquery.SchemaField("Team","STRING",mode= "NULLABLE"),
  bigquery.SchemaField("Performance","RECORD",mode= "REPEATED"
    fields=(
        bigquery.SchemaField("MatchNo","INTEGER",mode= "NULLABLE"),
        bigquery.SchemaField("Runs","INETGER",mode= "NULLABLE"),
        bigquery.SchemaField("Wickets","STRING",mode= "NULLABLE"),
        bigquery.SchemaField("Catches","INETGER",mode= "NULLABLE")

    )
  ),
  
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=biguery.SourceFormat.NEWLINE_DELIMITED_JSON,    
)
uri = "gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_2.csv"

table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_uri(uri, table_id, job_config=job_config) 
job.result()  # wait for the job to complete  

```

## Date partitioning
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    
    # This is needed if table doesn't exist, but won't hurt otherwise:
    time_partitioning=bigquery.table.TimePartitioning(type_="DAY",field="transaction_date")
)

# Include target partition in the table id:
table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result()  # wait for the job to complete # Wait for job to finish
```
## Date partitioning with partition value
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    # This is needed if table doesn't exist, but won't hurt otherwise:
    time_partitioning=bigquery.table.TimePartitioning(type_="DAY",field="transaction_date")
)

# Include target partition in the table id:
table_id = "qwiklabs-gcp-00-73bbdba69620.test.test$20201021" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result()  # wait for the job to complete # Wait for job to finish
```

## Integer partitioning
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    # This is needed if table doesn't exist, but won't hurt otherwise:
    range_partitioning=bigquery.table.RangePartitioning(range_=bigquery.table.PartitionRange(start=1,end=4,interval=1),field="transaction_id")
)

# Include target partition in the table id:
table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result()  # wait for the job to complete # Wait for job to finish
```


## Spark DataFrame

## connect to bigquery using pyspark
https://github.com/GoogleCloudDataproc/spark-bigquery-connector
```python
# Globally
spark.conf.set("credentialsFile", "</path/to/key/file>")
# Per read/Write
spark.read.format("bigquery").option("credentialsFile", "</path/to/key/file>")
```

sample df
```python
from pyspark.sql import SparkSession
import datetime
spark=SparkSession.builder.getOrCreate()

records = [
    {"transaction_id": 1, "transaction_date": datetime.date(2021, 10, 24)},
    {"transaction_id": 2, "transaction_date": datetime.date(2021, 10, 24)},
    {"transaction_id": 3, "transaction_date": datetime.date(2021, 10, 24)},
]
df=spark.createDataFrame(records)
```
## reading data from bigquery with spark dataframe
```python
df = spark.read \
  .format("bigquery") \
  .option("table", f"{project_id}.{dataset}.{table}") \
  .load()
```

## Read using sql query
```python
#In order to use this feature the following configurations MUST be set
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","<dataset>") # dataset which has table creation access

sql = """
  SELECT tag, COUNT(*) c
  FROM (
    SELECT SPLIT(tags, '|') tags
    FROM `bigquery-public-data.stackoverflow.posts_questions` a
    WHERE EXTRACT(YEAR FROM creation_date)>=2014
  ), UNNEST(tags) tag
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
  """
df = spark.read.format("bigquery").load(sql)
df = spark.read.format("bigquery").option("query", sql).load()
df.show()
```

## filter on column
```python
spark.read.bigquery("bigquery-public-data:samples.shakespeare")\
  .select("word")\
  .where("word = 'Hamlet' or word = 'Claudius'")\
  .collect()
```
```python
table = "bigquery-public-data.wikipedia.pageviews_2019"
df_wiki_pageviews = spark.read \
  .format("bigquery") \
  .option("table", table) \
  .option("filter", "datehour >= '2019-01-01' AND datehour < '2019-01-08'") \
  .load()
```

## writing spark dataframe
```python
## create if not exists and write to partitioned table which is partitioned on date
df.write \
   .format("bigquery") \
   .option("table",f"{project_id}.{dataset}.{table}") \
   .option("temporaryGcsBucket", gcs_bucket)\
   .mode('append') \
   .save()

#or

df.write.format("bigquery").save(f"{project_id}.{dataset}.{table}")
```

## Date partitioning
```python
## create if not exists and write to partitioned table which is partitioned on date
df.write \
   .format("bigquery") \
   .option("table",f"{project_id}.{dataset}.{table}") \
   .option("partitionField", "transaction_date") \
   .option("partitionType", "DAY") \
   .option("temporaryGcsBucket", gcs_bucket)\
   .mode('overwrite') \
   .save()
```

## Date partition with partition value defined
```python
## create if not exists and overwrite certain partition partitioned table
df.write \
   .format("bigquery") \
   .option("table",f"{project_id}.{dataset}.{table}") \
   .option("partitionField", "transaction_date") \
   .option("partitionType", "DAY") \
   .option("temporaryGcsBucket", gcs_bucket)\
   .mode('overwrite') \
   .save()
   
#or
  
df.write \
   .format("bigquery") \
   .option("table",f"{project_id}.{dataset}.{table}") \
   .option("partitionField", "transaction_date") \
   .option("partitionType", "DAY") \
   .option("temporaryGcsBucket", gcs_bucket)\
   .option("datePartition", "20211024") \
   .mode('overwrite') \
   .save()
```
