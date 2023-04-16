Get the tables and metadata for tables in the dataset
```sql
select * from 
`myDataset.__TABLES__`
```

Returns metadata for the specified project and region.
```sql
SELECT * FROM myProject.`region-us`.INFORMATION_SCHEMA.TABLES;
```

Returns metadata for tables in a single dataset.
```sql
SELECT * FROM myDataset.INFORMATION_SCHEMA.TABLES;
```

Returns metadata for columns in a single dataset.
```sql
SELECT * FROM myDataset.INFORMATION_SCHEMA.COLUMNS;
```

Returns metadata for partition columns in a single dataset.
```sql
SELECT * FROM myDataset.INFORMATION_SCHEMA.COLUMNS
WHERE is_partitioning_column='YES';
```

The following INFORMATION_SCHEMA views support dataset qualifiers:
- COLUMNS
- COLUMN_FIELD_PATHS
- PARAMETERS
- PARTITIONS
- ROUTINES
- ROUTINE_OPTIONS
- TABLES
- TABLE_OPTIONS
- VIEWS
- SEARCH_INDEXES
- SEARCH_INDEX_COLUMNS

Return metadat of all datasets in a region or project
```sql
SELECT * FROM myProject.INFORMATION_SCHEMA.SCHEMATA;
SELECT * FROM region-us.INFORMATION_SCHEMA.SCHEMATA;
```

## External table
```sql
CREATE EXTERNAL TABLE datset.table
OPTIONS(
    format = "CSV",
    uris=['gs://test_demo/test.csv']
    --OR uris = ['gs://test_demo/*.csv] - wildcard pattern
);
```

## standard sql queries
Providing "#standardSQL" will understand it is standard SQL query
```sql
#standardSQL
SELECT * FROM project.dataset.table;
```
## legacy sql queries
Providing "#legacySQL" will understand it is legacy SQL query
```sql
#legacySQL
SELECT * FROM project.dataset.table;
```

## Query multiple tables with wildcard
```sql
SELECT 
vendor_id,
max(passenger_count) as max_pass_count 
FROM 
`bigquery-public-data.new_york_taxi_trips.tlc_green_trips_20*` 
group by 1
```
## Query multiple tables with wildcard _TABLE_SUFFIX
Select only tables that match has prefix in the range mentioned in _TABLE_SUFFIX
```sql
SELECT 
*
FROM 
`bigquery-public-data.noaa_gsod.gsod19*` 
WHERE _TABLE_SUFFIX BETWEEN '29' and '35';
```

## Access historical data
Get 2days back data in the table 
```sql
SELECT * FROM `project.dataset.table`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 2 DAY);
```

## Create partition table on Month
```sql
CREATE OR REPLACE TABLE project.dataset.table
(
    id INT64,
    name STRING,
    start_date TIMESTAMP
)
PARTITION BY
    TIMESTAMP_TRUNC(start_time,MONTH);
```

## Create partition table on interger column
```sql
CREATE OR REPLACE TABLE project.dataset.table
(
    id INT64,
    name STRING,
    start_date TIMESTAMP
)
PARTITION BY
    RANGE_BUCKET(id,GENERATE_ARRAY(0,123456,10000));
```

# DDL

***[Bigquery DDL doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language)***

# DML
***[Bigquery DML doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax)***