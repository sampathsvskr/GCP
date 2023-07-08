
-- nested records from arrays of structs

CREATE OR REPLACE TABLE `demo.restaurant` AS (
SELECT "North America" as region, [
STRUCT("Cafe Pacific" AS name, "North York" AS location, STRUCT(["European", "Casual", "Wine bar"] AS cuisine_array, "100 $" AS price_range, False AS has_delivery) AS basic_info),
STRUCT("Boston Pizza" AS name, "Toronto" AS location, STRUCT(["Italian", "Pizza", "Fast-food"] AS cuisine_array, "50 $" AS price_range, True AS has_delivery) AS basic_info),
STRUCT("Spice on the Streets" AS name, "New York" AS location, STRUCT(["Indian", "Casual"] AS cuisine_array, "50 $" AS price_range, True AS has_delivery) AS basic_info),
STRUCT("Sushi Bar" AS name, "LA" AS location, STRUCT(["Japanese", "Sushi", "Casual"] AS cuisine_array, "150 $" AS price_range, False AS has_delivery) AS basic_info)] AS restaurant

UNION ALL

SELECT "Europe" as region, [
STRUCT("Pizza Pizza" AS name, "Paris" AS location, STRUCT(["Pizza"] AS cuisine_array, "200 $" AS price_range, False AS has_delivery) AS basic_info),
STRUCT("Cafe Coffe Day" AS name, "London" AS location, STRUCT(["French", "Bistro"] AS cuisine_array, "60 $" AS price_range, False AS has_delivery) AS basic_info)] AS restaurant);

-- query
-- get restaurants in each region with the cuisine type and price range which has delivery
SELECT region, restaurant_data.name, cuisine , restaurant_data.basic_info.price_range
FROM `demo.restaurant`, 
    UNNEST(restaurant) as restaurant_data, 
    UNNEST(restaurant_data.basic_info.cuisine_array) as cuisine
WHERE restaurant_data.basic_info.has_delivery = True

-- get and cuisine types in that region
SELECT region,  cuisine 
FROM `demo.restaurant`, 
    UNNEST(restaurant) as restaurant, 
    UNNEST(restaurant.basic_info.cuisine_array) as cuisine

-- get resturant names with region
SELECT region, restaurant.name 
FROM `demo.restaurant`, 
    UNNEST(restaurant) as restaurant