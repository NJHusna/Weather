from pathlib import Path

from dagster_dbt import DbtProject

weather_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "weather").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)