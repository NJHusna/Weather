{{config(materialized='table')}}

WITH 
dim_weather AS (
    SELECT DISTINCT 
        weather_id,
        weather,
        description
    FROM {{ref('silver_weather')}}
)

SELECT
    *
FROM dim_weather
