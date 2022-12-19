CREATE SCHEMA if not exists `{project}.{dataset}`;

CREATE TABLE if not exists {dataset}.{table} 
(
id int64,
name string,
location string
);
