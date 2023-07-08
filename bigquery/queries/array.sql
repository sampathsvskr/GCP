--Create a simple array with square brackets

--  Array Data
CREATE OR REPLACE TABLE `demo.restaurant_cuisine` AS (
SELECT "Cafe Pacific" AS name, "North York" AS location, ["European", "Casual", "Wine bar"] AS cuisine_array
UNION ALL
SELECT "Boston Pizza" AS name, "Toronto" AS location, ["Italian", "Pizza", "Fast-food"] AS cuisine_array
UNION ALL
SELECT "Spice on the Streets" AS name, "New York" AS location, ["Indian", "Casual", "Street-food"] AS cuisine_array
UNION ALL
SELECT "Sushi Bar" AS name, "LA" AS location, ["Japanese", "Sushi", "Casual"] AS cuisine_array);


-- UNNEST to flatten/explode an array
SELECT name, location, cuisine
FROM `demo.restaurant_cuisine`, UNNEST(cuisine_array) AS cuisine;

-- Using array column in where clause
SELECT name, location
FROM `demo.restaurant_cuisine`
WHERE 'Japanese' in UNNEST(cuisine_array);


-- Count the elements in an array with ARRAY_LENGTH

SELECT name, ARRAY_LENGTH(cuisine_array) AS number_of_label
FROM `demo.restaurant_cuisine`;

SELECT name, location, ARRAY_LENGTH(cuisine_array)
FROM `demo.restaurant_cuisine`
WHERE 'Japanese' in UNNEST(cuisine_array);

