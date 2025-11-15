import logging
import os
from datetime import datetime

import click

from .git_clients.gh_archive_client import GHArchiveClient
from .paths import BRONZE_DATA_PATH
from .utils import (
    decompress_gzip_data,
    generate_file_name,
    get_first_and_last_day_of_month,
    save_data_to_file,
)

client = GHArchiveClient()
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--start_date", help="The start date for the data dump in YYYY-MM-DD format."
)
@click.option("--end_date", help="The end date for the data dump in YYYY-MM-DD format.")
@click.option(
    "--output_dir",
    default=BRONZE_DATA_PATH,
    help="The output directory where the decompressed files will be saved.",
)
def run(start_date: str, end_date: str, output_dir: str) -> None:
    if not start_date or not end_date:
        start_date, end_date = get_first_and_last_day_of_month(
            year = datetime.now().year, 
            month = datetime.now().month
        )
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    os.makedirs(output_dir, exist_ok=True)

    for daily_data, current_date in client.get_events_dump(start_date, end_date):
        decompressed_data = decompress_gzip_data(daily_data)
        filename = generate_file_name(current_date)
        filepath = os.path.join(output_dir, filename)
        save_data_to_file(decompressed_data, filepath)

        logger.info(f"Saved dump to {filepath}")


if __name__ == "__main__":
    run()
