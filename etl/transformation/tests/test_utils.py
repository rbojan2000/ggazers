from datetime import date

from src.paths import ACTORS_PATH, REPOS_PATH
from src.utils import build_paths


def test_build_paths_single_day_actors():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "actors", parts_per_date=24)

    assert len(paths) == 24
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[23] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_23.jsonl"


def test_build_paths_single_day_repos():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "repos", parts_per_date=24)

    assert len(paths) == 24
    assert paths[0] == f"{REPOS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[23] == f"{REPOS_PATH}/2025_11_01/2025_11_01_23.jsonl"


def test_build_paths_multiple_days():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 3)

    paths = build_paths(start_date, end_date, "actors", parts_per_date=24)

    assert len(paths) == 72  # 3 days * 24 parts
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[24] == f"{ACTORS_PATH}/2025_11_02/2025_11_02_0.jsonl"
    assert paths[48] == f"{ACTORS_PATH}/2025_11_03/2025_11_03_0.jsonl"


def test_build_paths_custom_parts_per_date():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "actors", parts_per_date=10)

    assert len(paths) == 10
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[9] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_9.jsonl"


def test_build_paths_month_boundary():
    start_date = date(2025, 1, 31)
    end_date = date(2025, 2, 1)

    paths = build_paths(start_date, end_date, "repos", parts_per_date=24)

    assert len(paths) == 48
    assert paths[0] == f"{REPOS_PATH}/2025_01_31/2025_01_31_0.jsonl"
    assert paths[24] == f"{REPOS_PATH}/2025_02_01/2025_02_01_0.jsonl"
