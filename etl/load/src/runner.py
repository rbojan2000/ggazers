import logging

import click
from src.processor import DataProcessor

logger = logging.getLogger(__name__)

processor = DataProcessor()


@click.command()
@click.option("--period_start_date", help="The start date for the period in YYYY-MM-DD format.")
@click.option("--period_end_date", help="The end date for the period in YYYY-MM-DD format.")
@click.option(
    "--dataset",
    required=True,
    type=click.Choice(["repo_level_stats"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(period_start_date: str, period_end_date: str, dataset: str) -> None:

    processor.init_spark_session()

    match dataset:
        case "repo_level_stats":
            processor.calculate_repo_level_stats(period_start_date, period_end_date)
        case _:
            raise ValueError(f"Unknown dataset: {dataset}")

    logger.info(f"Stats calculation for dataset: {dataset} completed successfully.")

    processor.terminate_spark_session()


if __name__ == "__main__":
    run()
