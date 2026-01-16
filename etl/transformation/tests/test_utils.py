from datetime import date
from unittest.mock import patch

from src.paths import ACTORS_PATH, REPOS_PATH
from src.utils import build_paths


def test_build_paths_single_day_actors():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    fake_files = [f"2025_11_01_{i}.jsonl" for i in range(24)]
    with patch("os.path.exists", return_value=True), patch("os.listdir", return_value=fake_files):
        paths = build_paths(start_date, end_date, "actors")

    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[23] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_23.jsonl"
    assert len(paths) == 24


def test_build_paths_single_day_repos():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    fake_files = [f"2025_11_01_{i}.jsonl" for i in range(24)]
    with patch("os.path.exists", return_value=True), patch("os.listdir", return_value=fake_files):
        paths = build_paths(start_date, end_date, "repos")

    assert len(paths) == 24
    assert paths[0] == f"{REPOS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[23] == f"{REPOS_PATH}/2025_11_01/2025_11_01_23.jsonl"


def test_build_paths_multiple_days():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 3)

    fake_files_1 = [f"2025_11_01_{i}.jsonl" for i in range(24)]
    fake_files_2 = [f"2025_11_02_{i}.jsonl" for i in range(24)]
    fake_files_3 = [f"2025_11_03_{i}.jsonl" for i in range(24)]

    def listdir_side_effect(path):
        if "2025_11_01" in path:
            return fake_files_1
        elif "2025_11_02" in path:
            return fake_files_2
        elif "2025_11_03" in path:
            return fake_files_3
        return []

    with patch("os.path.exists", return_value=True), patch("os.listdir", side_effect=listdir_side_effect):
        paths = build_paths(start_date, end_date, "actors")

    assert len(paths) == 72  # 3 days * 24 parts
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[24] == f"{ACTORS_PATH}/2025_11_02/2025_11_02_0.jsonl"
    assert paths[48] == f"{ACTORS_PATH}/2025_11_03/2025_11_03_0.jsonl"


def test_build_paths_custom_parts_per_date():
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    fake_files = [f"2025_11_01_{i}.jsonl" for i in range(10)]
    with patch("os.path.exists", return_value=True), patch("os.listdir", return_value=fake_files):
        paths = build_paths(start_date, end_date, "actors")

    assert len(paths) == 10
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_0.jsonl"
    assert paths[9] == f"{ACTORS_PATH}/2025_11_01/2025_11_01_9.jsonl"


def test_build_paths_month_boundary():
    start_date = date(2025, 1, 31)
    end_date = date(2025, 2, 1)

    fake_files_1 = [f"2025_01_31_{i}.jsonl" for i in range(24)]
    fake_files_2 = [f"2025_02_01_{i}.jsonl" for i in range(24)]

    def listdir_side_effect(path):
        if "2025_01_31" in path:
            return fake_files_1
        elif "2025_02_01" in path:
            return fake_files_2
        return []

    with patch("os.path.exists", return_value=True), patch("os.listdir", side_effect=listdir_side_effect):
        paths = build_paths(start_date, end_date, "repos")

    assert len(paths) == 48
    assert paths[0] == f"{REPOS_PATH}/2025_01_31/2025_01_31_0.jsonl"
    assert paths[24] == f"{REPOS_PATH}/2025_02_01/2025_02_01_0.jsonl"
