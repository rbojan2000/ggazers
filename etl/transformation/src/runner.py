from datetime import datetime

import click
from pyspark.sql import SparkSession

from .paths import DATA_PATH
from .processor import DataProcessor
from .utils import get_first_and_last_day_of_month

spark = (
    SparkSession.builder.appName("transformation")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ggazers.type", "hadoop")
    .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
    .getOrCreate()
)


@click.command()
@click.option("--start_date", help="The start date for the data dump in YYYY-MM-DD format.")
@click.option("--end_date", help="The end date for the data dump in YYYY-MM-DD format.")
@click.option(
    "--dataset_name",
    required=True,
    type=click.Choice(["actors", "github_events", "repos"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(start_date: str, end_date: str, dataset_name: str) -> None:

    if not start_date or not end_date:
        start_date, end_date = get_first_and_last_day_of_month(
            year=datetime.now().year, month=datetime.now().month
        )
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    match dataset_name.lower():
        case "actors":
            DataProcessor.transform_actors(spark, start_date, end_date)
        case "github_events":
            pass
        case "repos":
            pass
        case _:
            raise ValueError(f"Unknown dataset: {dataset_name}")


if __name__ == "__main__":
    run()
