import logging
from datetime import datetime

import click
from src.processor import DataProcessor
from src.utils import get_first_and_last_day_of_month

logger = logging.getLogger(__name__)

data_processor = DataProcessor()


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

    data_processor.init_spark_session()
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

    data_processor.terminate_spark_session()


if __name__ == "__main__":
    run()
