import calendar
import logging
from datetime import date, datetime, timezone

import click
from src.processor import Processor
from src.transformer import Transformer

logger = logging.getLogger(__name__)

SESSION_GAP_SECONDS = 8 * 60 * 60  # 8 hours
transformer = Transformer(SESSION_GAP_SECONDS)
processor = Processor(transformer)


@click.command()
@click.option(
    "--start_date",
    help="The start date for the data dump in YYYY-MM-DD format.",
    default=date(datetime.now().year, datetime.now(timezone.utc).month, 1),
)
@click.option(
    "--end_date",
    help="The end date for the data dump in YYYY-MM-DD format.",
    default=date(
        datetime.now().year,
        datetime.now(timezone.utc).month,
        calendar.monthrange(datetime.now().year, datetime.now(timezone.utc).month)[1],
    ),
)
@click.option(
    "--dataset",
    required=True,
    type=click.Choice(["actors", "github_events", "repos"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(start_date: str, end_date: str, dataset: str) -> None:
    logger.info(f"Starting transformation for dataset: {dataset}, from {start_date} to {end_date}.")
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    processor.init_spark_session()
    match dataset.lower():
        case "actors":
            processor.process_actors(start_date, end_date)
        case "repos":
            processor.process_repos(start_date, end_date)
        case "github_events":
            processor.process_github_events(start_date, end_date)
        case _:
            raise ValueError(f"Unknown dataset: {dataset}")

    logger.info(f"Transformation for dataset: {dataset} completed successfully.")

    processor.terminate_spark_session()


if __name__ == "__main__":
    run()
