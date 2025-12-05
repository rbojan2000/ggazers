import logging
from datetime import datetime

import click
from pyspark.sql import SparkSession
from src.paths import DATA_PATH
from src.processor import DataProcessor
from src.utils import get_first_and_last_day_of_month

logger = logging.getLogger(__name__)

spark_session: SparkSession = (
    SparkSession.builder.appName("transformation")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ggazers.type", "hadoop")
    .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
    .getOrCreate()
)
data_processor = DataProcessor(spark_session)


@click.command()
@click.option("--start_date", help="The start date for the data dump in YYYY-MM-DD format.")
@click.option("--end_date", help="The end date for the data dump in YYYY-MM-DD format.")
@click.option(
    "--dataset",
    required=True,
    type=click.Choice(["actors", "github_events", "repos"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(start_date: str, end_date: str, dataset: str) -> None:
    logger.info(f"Starting transformation for dataset: {dataset}, from {start_date} to {end_date}.")
    if not start_date or not end_date:
        start_date, end_date = get_first_and_last_day_of_month(
            year=datetime.now().year, month=datetime.now().month
        )
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    match dataset.lower():
        case "actors":
            data_processor.process_actors(start_date, end_date)
        case "repos":
            data_processor.process_repos(start_date, end_date)
        case "github_events":
            data_processor.process_github_events(start_date, end_date)
        case _:
            raise ValueError(f"Unknown dataset: {dataset}")

    logger.info(f"Transformation for dataset: {dataset} completed successfully.")
    spark_session.stop()


if __name__ == "__main__":
    run()
