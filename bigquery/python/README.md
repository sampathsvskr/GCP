
## Referal links
https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJobConfig
https://www.programcreek.com/python/example/115622/google.cloud.bigquery.QueryJobConfig
https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/notebooks/rendered/bigquery-basics.md
https://github.com/GoogleCloudDataproc/spark-bigquery-connector


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

df = query_job.to_dataframe()
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
table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result() # Wait for job to finish
```
Date partitioning with partition value
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    # This is needed if table doesn't exist, but won't hurt otherwise:
    time_partitioning=bigquery.table.TimePartitioning(type_="DAY",field="transaction_date")
)

# Include target partition in the table id:
table_id = "qwiklabs-gcp-00-73bbdba69620.test.test$20201021" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result() # Wait for job to finish
```

Integer partitioning
```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    # This is needed if table doesn't exist, but won't hurt otherwise:
    range_partitioning=bigquery.table.RangePartitioning(range_=bigquery.table.PartitionRange(start=1,end=4,interval=1),field="transaction_id")
)

# Include target partition in the table id:
table_id = f"{project_id}.{dataset}.{table}" 
job = client.load_table_from_dataframe(df, table_id, job_config=job_config) # Make an API request
job.result() # Wait for job to finish
```


## Spark DataFrame

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

Read using sql query
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

filter on column
```python
spark.read.bigquery("bigquery-public-data:samples.shakespeare")\
  .select("word")\
  .where("word = 'Hamlet' or word = 'Claudius'")\
  .collect()
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

Date partitioning
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

Date partition with partition value defined
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