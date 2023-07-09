-- Stored Procedure Syntax

CREATE PROCEDURE dataset_name.procedure_name
BEGIN
-- statements here
END


-- Stored procedure example
CREATE OR REPLACE PROCEDURE demo.create_customer()
BEGIN
  DECLARE id STRING;
  SET id = GENERATE_UUID();
  INSERT INTO demo.customers (id)
    VALUES(id);
  SELECT FORMAT("Created customer %s", id);
END


-- Persistent udf function

CREATE OR REPLACE FUNCTION
  demo.cleanse_string_test (text STRING)
  RETURNS STRING
  AS (REGEXP_REPLACE(LOWER(TRIM(text)), '[^a-zA-Z0-9 ]+', ''));
  
WITH strings AS
  (SELECT '  Hello, World!!!  ' AS text
  UNION ALL
  SELECT ' I am $Special$ STrINg ' AS text
  UNION ALL
  SELECT ' John, Doe ' AS text)
SELECT text
     , `bigquery-demo-347418.demo.cleanse_string_test`(text) AS clean_text
FROM strings;



-- Temporary UDf
CREATE TEMP FUNCTION cleanse_string (text STRING)
  RETURNS STRING
  AS (REGEXP_REPLACE(LOWER(TRIM(text)), '[^a-zA-Z0-9 ]+', ''));
WITH strings AS
  (SELECT '  Hello, World!!!  ' AS text
  UNION ALL
  SELECT ' I am $Special$ STrINg ' AS text
  UNION ALL
  SELECT ' John, Doe ' AS text)
SELECT text
     , cleanse_string(text) AS clean_text
FROM strings;



-- Table Valued function  -> returns table
CREATE OR REPLACE TABLE FUNCTION demo.names_by_year(y INT64)
AS
  SELECT year, name, SUM(number) AS total
  FROM `bigquery-public-data.usa_names.usa_1910_current`
  WHERE year = y
  GROUP BY year, name
  
  
-- TVF example
SELECT * FROM demo.names_by_year(1950)
  ORDER BY total DESC
  LIMIT 5

-- Routines Info
SELECT *
  FROM demo.INFORMATION_SCHEMA.ROUTINES

---

declare res_name string;
execute immediate
"select min(restaurant.name) from `big-query-355306.mydataset.restaurant`, UNNEST(restaurant) as restaurant" into res_name;

select * from `big-query-355306.mydataset.restaurant` ,UNNEST(restaurant) as restaurant
where restaurant.name != res_name