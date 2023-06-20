
## Create dataset
```
bq --location=us-east4 mk \
--dataset \
--default_table_expiration=3600 \
--description="demo dataset" \
<project>:<dataset_name>
```

## Create table
```
bq --location=us-east4 mk \
--table \
--expiration=3600 \
--description="demo table" \
<dataset_name>.<table_name> \
id:integer,name:string,location:string
```

```
bq  mk \
--table \
--schema sample_schema.json \
<dataset_name>.<table_name> 
```

```
bq  mk \
--table \
--schema sample_schema.json \
--time_partitioning_field transaction_time \
<dataset_name>.<table_name> 
```

## Load csv data in gcs to bigquery

```
bq load \
--source_format=CSV \
--autodetect \
--noreplace  \
nyctaxi.2018trips \
gs://cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_2.csv
```

## Load csv data in cloudshell local file system to bigquery

```
bq load \
--source_format=CSV \
--skip_leading_rows=1 \
nyctaxi.2018trips \
/home/cloud-training/OCBL013/nyc_tlc_yellow_trips_2018_subset_2.csv

```

## Query table

```
bq query --location=us-east4 \
--use_legacy_sql=false \
"
SELECT * FROM `project.dataset.table`;
"
```

## Parameterized queries
```
bq query \
--use_legacy_sql=false \
--parameter=country::IN \  #param_name:datatype:value  , for string not necessary to specify
--parameter=date:DATE:2021-10-10 \
'
SELECT 
country_name,
EXTRACT(month FROM date) as month,
EXTRACT(year FROM date) as year,
SUM(cumulative_confirmed) as cum_sum_cases
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE country_code=@country and date < @date 
GROUP BY 1,2,3
ORDER BY 1,2,3;
'
```

## Positional Parameterized queries
```
bq query \
--use_legacy_sql=false \
--parameter=::IN \  #:datatype:value  , for string not necessary to specify
--parameter=:DATE:2021-10-10 \
'
SELECT 
country_name,
EXTRACT(month FROM date) as month,
EXTRACT(year FROM date) as year,
SUM(cumulative_confirmed) as cum_sum_cases
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
WHERE country_code=? and date < ? 
GROUP BY 1,2,3
ORDER BY 1,2,3;
'
```

## Get the deleted table.
We can get the deleted table data if it is less than 7 days from deletion.
It represnets current time - 3600000
```
bq cp dataset_demo.table@-3600000 dataset_demo.table_restored
```

## Schedule script

bq query \
--use_legacy_sql=false \
--display_name="Scheduled script" \
--location="us-east4" \
--schedule="every 24 hours"
'
CREATE OR REPLACE project.dataet.table AS 
SELECT 
country_name,
EXTRACT(month FROM date) as month,
EXTRACT(year FROM date) as year,
SUM(cumulative_confirmed) as cum_sum_cases
FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
GROUP BY 1,2,3
ORDER BY 1,2,3;
'
```
