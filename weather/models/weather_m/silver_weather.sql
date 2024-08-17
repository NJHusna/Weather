{{config(materialized='table')}}

WITH 
s_weather AS (
    SELECT 
    id,
    longitude,
    latitude,
    weather_id,
    weather,
    description,
    temperature,
    min_temperature,
    max_temperature,
    pressure,
    humidity,
    sea_level,
    grnd_level,
    wind_speed,
    wind_direction,
    wind_gust,
    cloudiness_perc,
    datetime,
    sunrise_time,
    sunset_time,
    city_id,
    city_name,
    CASE 
        WHEN city_name = 'Lawas' AND country_code = 'PH' THEN 'MY'
        ELSE country_code
    END AS country_code,
    CONCAT(city_id, '.', latitude, '.', longitude) as location_id
    FROM {{ref('bronze_weather')}}
)

SELECT *
FROM s_weather