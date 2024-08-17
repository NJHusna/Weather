# weather_asset.py
import os
import psycopg2
import httpx
import json
from dotenv import load_dotenv
from dagster import resource, asset

@resource
def db_resource():
    load_dotenv()
    connection = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    return connection, connection.cursor()

@resource
def api_keys():
    load_dotenv()
    api_key_c = os.getenv("api_key_c")
    api_key_w = os.getenv("api_key_w")
    return api_key_c, api_key_w

@asset(required_resource_keys={"api_keys"})
def fetch_cities(context):
    api_key_c = context.resources.api_keys[0]
    base_url_c = "https://api.countrystatecity.in/v1/countries/MY/cities"
    headers = {"X-CSCAPI-KEY": api_key_c}
    c_response = httpx.get(base_url_c, headers=headers)
    c_response.raise_for_status()
    cities = [city['name'] for city in c_response.json()]
    context.log.info(f"Fetched {len(cities)} cities.")
    context.log.info(f"Cities: {cities}")  # Log the list of cities
    return cities
    # return [city['name'] for city in c_response.json()]

@asset(required_resource_keys={"db_resource"})
def create_weather_table(context):
    conn, cursor = context.resources.db_resource
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_jsondata (
            id SERIAL PRIMARY KEY,
            weather_data JSONB NOT NULL
        )
    """)
    conn.commit()
    
@asset(required_resource_keys={"db_resource", "api_keys"}, non_argument_deps={"create_weather_table"})
def fetch_store_weather_data(context, fetch_cities):
    conn, cursor = context.resources.db_resource
    api_key_w = context.resources.api_keys[1]
    cities = fetch_cities  # Correctly refer to fetch_cities

    base_url_w = "https://api.openweathermap.org/data/2.5/weather"

    for city in cities:
        try:
            params = {'appid': api_key_w, 'q': city, 'units': "metric"}
            response = httpx.get(base_url_w, params=params)
            response.raise_for_status()
            weather_data = response.json()

            context.log.info(f"Weather data for {city}: {weather_data}")

            cursor.execute("INSERT INTO weather_jsondata (weather_data) VALUES (%s)", (json.dumps(weather_data),))
            conn.commit()
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
            context.log.error(f"Error fetching or storing data for {city}: {e}")

    cursor.close()
    conn.close()