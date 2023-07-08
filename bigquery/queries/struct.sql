
-- Create a simple struct

CREATE OR REPLACE TABLE `demo.struct_restaurant_cuisine` AS (
SELECT "Cafe Pacific" AS name, "North York" AS location,
STRUCT(["European", "Casual", "Wine bar"] AS cuisine_array, "100 $" AS price_range, False AS has_delivery) AS basic_info
UNION ALL
SELECT "Boston Pizza" AS name, "Toronto" AS location,
STRUCT(["Italian", "Pizza", "Fast-food"] AS cuisine_array, "50 $" AS price_range, True AS has_delivery) AS basic_info
UNION ALL
SELECT "Spice on the Streets" AS name, "New York" AS location,
STRUCT(["Indian", "Casual"] AS cuisine_array, "50 $" AS price_range, True AS has_delivery) AS basic_info
UNION ALL
SELECT "Sushi Bar" AS name, "LA" AS location,
STRUCT(["Japanese", "Sushi", "Casual"] AS cuisine_array, "150 $" AS price_range, False AS has_delivery) AS basic_info);



-- Queries
-- fetch struct fields with DOT(.)
SELECT name, location, basic_info.price_range AS price_range, cuisine
FROM `demo.struct_restaurant_cuisine`, UNNEST(basic_info.cuisine_array) AS cuisine

SELECT name, location, basic_info.price_range AS price_range, cuisine
FROM `demo.struct_restaurant_cuisine`, UNNEST(basic_info.cuisine_array) AS cuisine
WHERE basic_info.has_delivery = True
AND "Casual" IN UNNEST(basic_info.cuisine_array)

--Regroup cuisine into array for final query result
with casual_delivery as 
(
    SELECT name, location, basic_info.price_range AS price_range, cuisine
FROM `demo.struct_restaurant_cuisine`, UNNEST(basic_info.cuisine_array) AS cuisine
WHERE basic_info.has_delivery = True
AND "Casual" IN UNNEST(basic_info.cuisine_array)

)

SELECT name, location, price_range, ARRAY_AGG(cuisine) AS cuisine
FROM casual_delivery
GROUP BY name, location, price_range;


