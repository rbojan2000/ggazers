import calendar
import logging
from datetime import date, datetime, timezone

import click
import dotenv
from src.git_clients.gh_archive_client import GHArchiveClient
from src.git_clients.github_client import GithubClient
from src.processor import Processor

SLEEP_ON_FAILURE = 4 * 60 * 60  # 4 hours
dotenv.load_dotenv()

logger = logging.getLogger(__name__)
gh_archive_client = GHArchiveClient()
github_client = GithubClient()
processor = Processor(gh_archive_client, github_client)


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
@click.option("--chunk_size", default=20, help="GraphQL query chunk size.")
def run(start_date: str, end_date: str, chunk_size: int = 20) -> None:
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    processor.run(start_date, end_date, chunk_size, SLEEP_ON_FAILURE)


if __name__ == "__main__":
    run()
