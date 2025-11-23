import logging
import os
import time
from datetime import datetime

import click
import dotenv

from .common import save_json, spill_bytes
from .git_clients.gh_archive_client import GHArchiveClient
from .git_clients.github_client import GithubClient
from .paths import ACTORS_PATH, GITHUB_EVENTS_PATH, REPOS_PATH
from .utils import (
    chunk_list,
    decompress_data,
    extract_repos_and_actors,
    generate_file_name,
    get_first_and_last_day_of_month,
)

SLEEP_ON_FAILURE = 4 * 60 * 60  # 4 hours
dotenv.load_dotenv()

logger = logging.getLogger(__name__)
gh_archive_client = GHArchiveClient()
github_client = GithubClient()


@click.command()
@click.option("--start_date", help="The start date for the data dump in YYYY-MM-DD format.")
@click.option("--end_date", help="The end date for the data dump in YYYY-MM-DD format.")
@click.option("--chunk_size", default=20, help="GraphQL query chunk size.")
def run(start_date: str, end_date: str, chunk_size: int = 20) -> None:
    if not start_date or not end_date:
        start_date, end_date = get_first_and_last_day_of_month(
            year=datetime.now().year, month=datetime.now().month
        )
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    for daily_part_data, current_date, part in gh_archive_client.get_events_dump(
        start_date, end_date
    ):
        decompressed_data = decompress_data(daily_part_data)
        filename = generate_file_name(current_date, part)
        filepath = os.path.join(GITHUB_EVENTS_PATH, current_date.strftime("%Y_%m_%d"), filename)
        spill_bytes(data=decompressed_data, filepath=filepath)

        repos, actors = extract_repos_and_actors(decompressed_data)
        logger.info(f"Extracted {len(repos)} unique repos and {len(actors)} unique actors.")

        for chunk_index, chunk in enumerate(chunk_list(repos, chunk_size)):
            logging.info(f"Processing repo chunk: #{chunk_index}")
            query = github_client.build_graphql_query(repos=chunk)
            result = github_client.run_query(query)
            if not result:
                logger.info("GraphQL query failed, trying REST API...")
                result = github_client.hit_rest_api("repos", chunk)

            if not result:
                logger.warning("All authentication attempts failed. Sleeping for 4 hours...")
                time.sleep(SLEEP_ON_FAILURE)

            filename = generate_file_name(current_date, part)
            filepath = os.path.join(REPOS_PATH, current_date.strftime("%Y_%m_%d"), filename)
            save_json(data=result, filepath=filepath)

        for chunk_index, chunk in enumerate(chunk_list(actors, chunk_size)):
            logging.info(f"Processing actor chunk: #{chunk_index}")
            query = github_client.build_graphql_query(actors=chunk)
            result = github_client.run_query(query)
            if not result:
                logger.info("GraphQL query failed, trying REST API...")
                result = github_client.hit_rest_api("users", chunk)

            if not result:
                logger.warning("All authentication attempts failed. Sleeping for 4 hours...")
                time.sleep(SLEEP_ON_FAILURE)

            filename = generate_file_name(current_date, part)
            filepath = os.path.join(ACTORS_PATH, current_date.strftime("%Y_%m_%d"), filename)
            save_json(data=result, filepath=filepath)


if __name__ == "__main__":
    run()
