
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