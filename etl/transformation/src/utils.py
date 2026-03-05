import os
from datetime import date, timedelta
from typing import List

from src.paths import ACTORS_PATH, GITHUB_EVENTS_PATH, REPOS_PATH


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
