{{config(materialized='table')}}

WITH dim_location AS (
    SELECT
        city_id,
        city_name,
        country_code,
        longitude,
        latitude,
        location_id
    FROM {{ref('silver_weather')}}
)
SELECT DISTINCT
    *
FROM dim_location