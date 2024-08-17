from dagster import repository, with_resources
from dagster_dbt import DbtCliResource
from .weather_asset import fetch_cities, create_weather_table, fetch_store_weather_data, db_resource, api_keys
from .dbt_assets import weather_dbt_assets
from .project import weather_project

@repository
def weather_repository():
    return [define_weather_job()]

def define_weather_job():
    return (
        with_resources(
            fetch_cities,
            resource_defs={
                "db_resource": db_resource,
                "api_keys": api_keys
            },
        )
        + with_resources(
            create_weather_table,
            resource_defs={
                "db_resource": db_resource
            },
        )
        + with_resources(
            fetch_store_weather_data,
            resource_defs={
                "db_resource": db_resource,
                "api_keys": api_keys
            },
        )
        + with_resources(
            weather_dbt_assets,
            resource_defs={
                "dbt": DbtCliResource(project_dir=weather_project.project_dir),
            }
        )
    )
