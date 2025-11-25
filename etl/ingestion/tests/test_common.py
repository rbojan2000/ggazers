import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from common import add_column, save_jsonl  # adjust import path


def test_add_column():
    data: List[Dict[str, Any]] = [
        {"login": "octocat", "name": "The Octocat"},
        {"login": "rbojan2000", "name": "Bojan Radovic"},
    ]

    ts = int(datetime(2025, 11, 25).timestamp())
    result = add_column(data, "ingested_at", ts)

    expected: List[Dict[str, Any]] = [
        {"login": "octocat", "name": "The Octocat", "ingested_at": ts},
        {"login": "rbojan2000", "name": "Bojan Radovic", "ingested_at": ts},
    ]

    assert result == expected


def test_save_jsonl(tmp_path: Path):
    data: List[Dict[str, Any]] = [
        {"login": "octocat", "name": "The Octocat"},
        {"login": "rbojan2000", "name": "Bojan Radovic"},
    ]

    file_path = tmp_path / "test.jsonl"

    save_jsonl(data, str(file_path))

    save_jsonl([{"login": "alice", "name": "Alice"}], str(file_path))

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
        loaded = [json.loads(line) for line in lines]

    assert loaded == [
        {"login": "octocat", "name": "The Octocat"},
        {"login": "rbojan2000", "name": "Bojan Radovic"},
        {"login": "alice", "name": "Alice"},
    ]
