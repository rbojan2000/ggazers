import calendar
import os
from datetime import date, timedelta
from typing import List, Tuple

from src.paths import ACTORS_PATH, GITHUB_EVENTS_PATH, REPOS_PATH


def get_first_and_last_day_of_month(year: int, month: int) -> Tuple[date, date]:
    first_day = date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def build_paths(start_date: date, end_date: date, dataset: str, parts_per_date: int = 24) -> List[str]:
    match dataset:
        case "actors":
            base_path = ACTORS_PATH
        case "repos":
            base_path = REPOS_PATH
        case "github_events":
            base_path = GITHUB_EVENTS_PATH
        case _:
            raise ValueError(f"Unknown dataset: {dataset}")

    paths = []
    current_date = start_date
    while current_date <= end_date:
        date_str = f"{current_date.year}_{current_date.month:02d}_{current_date.day:02d}"
        dir_path = f"{base_path}/{date_str}"
        if os.path.exists(dir_path):
            for fname in os.listdir(dir_path):
                if fname.endswith(".jsonl"):
                    paths.append(f"{dir_path}/{fname}")
        current_date += timedelta(days=1)
    return paths
