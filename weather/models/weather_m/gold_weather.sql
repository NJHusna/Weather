{{config(materialized='table')}}

WITH 
date_dimension AS (
    SELECT
        *
    FROM {{ref('dim_datetime')}}
),
dim_location AS (
    SELECT
        *
    FROM {{ref('dim_location')}}
),
dim_weather AS (
    SELECT
        *
    FROM {{ref('dim_weather')}}
),
weather_fact AS (
    SELECT
        *
    FROM {{ref('weather_fact')}}
)

SELECT
    wf.id,
    wf.date,
    dt.day,
    dt.day_of_week,
    dt.day_name,
    dt.day_of_year,
    dt.week_of_year,
    dt.month,
    dt.month_name,
    dt.quarter,
    dt.year,
    dt.is_weekday,
    wf.weather_id,
    dw.weather,
    dw.description,
    wf.temperature,
    wf.min_temperature,
    wf.max_temperature,
    wf.pressure,
    wf.humidity,
    wf.sea_level,
    wf.grnd_level,
    wf.wind_speed,
    wf.wind_direction,
    wf.wind_gust,
    wf.cloudiness_perc,
    wf.sunrise_time,
    wf.sunset_time,
    wf.location_id,
    dl.city_id,
    dl.city_name,
    dl.country_code,
    dl.longitude,
    dl.latitude
FROM weather_fact wf
JOIN date_dimension dt ON wf.date = dt.date
JOIN dim_weather dw ON wf.weather_id = dw.weather_id
JOIN dim_location dl ON wf.location_id = dl.location_id
