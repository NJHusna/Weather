{{config(materialized='table')}}

WITH 
weather AS (
    SELECT * FROM {{source('weather_raw', 'weather_jsondata')}}
),

weather_data AS (
    SELECT 
    id,
    weather_data ->> 'coord' as coordinates,
    weather_data ->> 'weather' as weather,
    weather_data ->> 'main' as main,
    weather_data ->> 'wind' as wind,
    weather_data ->> 'clouds' as clouds,
    weather_data ->> 'rain' as rain,
    weather_data ->> 'dt' as datetime,
    weather_data ->> 'sys' as sys,
    weather_data ->> 'id' as city_id,
    weather_data ->> 'name' as city_name
    FROM weather
)
SELECT
    id,
    (coordinates::jsonb->>'lon')::float as longitude,
    (coordinates::jsonb->>'lat')::float as latitude,
    (weather::jsonb->0->>'id')::int as weather_id,
    (weather::jsonb->0->>'main') as weather,
    (weather::jsonb->0->>'description') as description,
    (main::jsonb->>'temp') as temperature,
    (main::jsonb->>'temp_min')::float as min_temperature,
    (main::jsonb->>'temp_max')::float as max_temperature,
    (main::jsonb->>'pressure')::int as pressure,
    (main::jsonb->>'humidity')::int as humidity,
    (main::jsonb->>'sea_level')::int as sea_level,
    (main::jsonb->>'grnd_level')::int as grnd_level,
    (wind::jsonb->>'speed')::float as wind_speed,
    (wind::jsonb->>'deg')::int as wind_direction,
    (wind::jsonb->>'gust')::float as wind_gust,
    (clouds::jsonb->>'all')::int as cloudiness_perc,
    to_timestamp((datetime)::bigint) as datetime,
    (sys::jsonb->>'country') as country_code,
    to_timestamp((sys::jsonb->>'sunrise')::bigint) as sunrise_time,
    to_timestamp((sys::jsonb->>'sunset')::bigint) as sunset_time,
    (city_id)::int as city_id,
    (city_name) as city_name
FROM weather_data

--  The weather field itself is an array ([]), containing one object ({}). 
-- Arrays in JSON are zero-indexed, which means the first element in the array has an index of 0.