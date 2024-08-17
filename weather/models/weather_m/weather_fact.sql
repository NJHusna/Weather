{{config(materialized='table')}}

WITH 
weather_fact AS (
    SELECT 
        id,
        datetime::DATE AS date,
        weather_id,
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
        sunrise_time,
        sunset_time,
        location_id
    FROM {{ref('silver_weather')}}
)

SELECT
    *
FROM weather_fact
