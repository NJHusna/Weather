from dagster import Definitions
from dagster_dbt import DbtCliResource

from .dbt_assets import weather_dbt_assets
from .project import weather_project
from .schedules import schedules
from .weather_asset import fetch_cities, create_weather_table, fetch_store_weather_data, db_resource, api_keys

# Defining all assets
all_assets = [
    fetch_cities,
    create_weather_table,
    fetch_store_weather_data,
    weather_dbt_assets,
]

# Providing necessary resources
defs = Definitions(
    assets=all_assets,
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=weather_project.project_dir),
        "db_resource": db_resource,
        "api_keys": api_keys,
        "create_weather_table": create_weather_table,  # Include this resource definition
        "fetch_cities": fetch_cities,  # Include this resource definition
    },
)