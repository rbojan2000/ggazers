import calendar
import gzip
import json
from datetime import date, datetime
from typing import Generator, List, Tuple


def get_first_and_last_day_of_month(year: int, month: int) -> Tuple[date, date]:
    first_day = date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def decompress_data(compressed_data: bytes) -> bytes:
    return gzip.decompress(compressed_data)


def generate_file_name(date: datetime, part: int) -> str:
    return f"{date.strftime('%Y_%m_%d')}_{part}.json"


def chunk_list(lst: List[str], chunk_size: int) -> Generator[List[str], None, None]:
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def extract_repos_and_actors(decompressed_data: bytes) -> Tuple[List[str], List[str]]:
    repos = set()
    actors = set()

    data_str = decompressed_data.decode("utf-8")

    for line in data_str.strip().split("\n"):
        if not line:
            continue

        event = json.loads(line)
        repo = event["repo"]["name"]
        actor = event["actor"]["login"]
        if "/" in actor:
            actor = actor.split("/")[0]
        repos.add(repo)
        actors.add(actor)

    return list(repos), list(actors)
