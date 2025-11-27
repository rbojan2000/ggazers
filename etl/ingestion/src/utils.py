import calendar
import gzip
import json
from datetime import date, datetime
from typing import Any, Dict, Generator, List, Tuple


def get_first_and_last_day_of_month(year: int, month: int) -> Tuple[date, date]:
    first_day = date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def decompress_data(compressed_data: bytes) -> List[Dict[str, Any]]:
    data_str = gzip.decompress(compressed_data).decode("utf-8")
    events: List[Dict[str, Any]] = []

    for line in data_str.strip().split("\n"):
        if not line:
            continue

        event = json.loads(line)
        events.append(event)

    return events


def generate_file_name(date: datetime, part: int) -> str:
    return f"{date.strftime('%Y_%m_%d')}_{part}.jsonl"


def chunk_list(lst: List[str], chunk_size: int) -> Generator[List[str], None, None]:
    for i in range(0, len(lst), chunk_size):
        yield lst[i : i + chunk_size]


def extract_repos_and_actors(data: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    repos = set()
    actors = set()

    for line in data:
        if not line:
            continue

        repo_info = line.get("repo")
        actor_info = line.get("actor")

        if not repo_info or not actor_info:
            continue

        repo_name = repo_info.get("name")
        actor_login = actor_info.get("login")

        if not repo_name or not actor_login:
            continue
        repo_owner = extract_owner_from_repo_full_name(repo_name)

        if "/" in actor_login:
            actor_login = actor_login.split("/")[0]

        repos.add(repo_name)
        actors.add(actor_login)
        actors.add(repo_owner)
    return list(repos), list(actors)


def extract_owner_from_repo_full_name(repo_full_name: str) -> str:
    return repo_full_name.split("/")[0] if "/" in repo_full_name else repo_full_name
