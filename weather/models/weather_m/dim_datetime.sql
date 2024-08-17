{{config(materialized='table')}}

WITH 
date_dimension AS (
    SELECT
        DISTINCT
        datetime::DATE AS date,
        EXTRACT(DAY FROM datetime) AS day,
        EXTRACT(DOW FROM datetime) AS day_of_week,
        TO_CHAR(datetime, 'Day') AS day_name,
        EXTRACT(DOY FROM datetime) AS day_of_year,
        EXTRACT(WEEK FROM datetime) AS week_of_year,
        EXTRACT(MONTH FROM datetime) AS month,
        TO_CHAR(datetime, 'Month') AS month_name,
        EXTRACT(QUARTER FROM datetime) AS quarter,
        EXTRACT(YEAR FROM datetime) AS year,
        CASE 
            WHEN EXTRACT(DOW FROM datetime) IN (0, 6) THEN FALSE
            ELSE TRUE
        END AS is_weekday
    FROM {{ref('silver_weather')}}
)

SELECT *
FROM date_dimension
