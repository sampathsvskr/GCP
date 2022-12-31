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

# DDL

***[Bigquery DDL doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language)***

# DML
***[Bigquery DML doc](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax)***