import gzip
import json
from datetime import date, datetime

from src.utils import (
    chunk_list,
    decompress_data,
    extract_repos_and_actors,
    generate_file_name,
    get_first_and_last_day_of_month,
)


def test_january_regular_year():
    first, last = get_first_and_last_day_of_month(2023, 1)
    assert first == date(2023, 1, 1)
    assert last == date(2023, 1, 31)


def test_february_regular_year():
    first, last = get_first_and_last_day_of_month(2023, 2)
    assert first == date(2023, 2, 1)
    assert last == date(2023, 2, 28)


def test_decompress_valid_data():
    obj = {
        "id": "4397712120",
        "type": "WatchEvent",
        "actor": {
            "id": 89842553,
            "login": "arsenydubrovin",
            "display_login": "arsenydubrovin",
            "gravatar_id": "",
            "url": "https://api.github.com/users/arsenydubrovin",
            "avatar_url": "https://avatars.githubusercontent.com/u/89842553?",
        },
        "repo": {
            "id": 197081291,
            "name": "iced-rs/iced",
            "url": "https://api.github.com/repos/iced-rs/iced",
        },
        "payload": {"action": "started"},
        "public": True,
        "created_at": "2025-11-01T00:00:00Z",
        "org": {
            "id": 54513237,
            "login": "iced-rs",
            "gravatar_id": "",
            "url": "https://api.github.com/orgs/iced-rs",
            "avatar_url": "https://avatars.githubusercontent.com/u/54513237?",
        },
    }

    json_lines = json.dumps(obj) + "\n"
    compressed = gzip.compress(json_lines.encode("utf-8"))

    result = decompress_data(compressed)

    assert result == [obj]


def test_generate_file_name():
    test_date = datetime(2023, 5, 15, 14, 30, 45)
    part = 0

    result = generate_file_name(test_date, part)

    assert result == "2023_05_15_0.jsonl"


def test_chunk_list_exact_division():
    test_list = ["a", "b", "c", "d", "e", "f"]

    chunks = list(chunk_list(test_list, 2))

    assert chunks == [["a", "b"], ["c", "d"], ["e", "f"]]


def test_chunk_list_remainder():
    test_list = ["a", "b", "c", "d", "e"]

    chunks = list(chunk_list(test_list, 2))

    assert chunks == [["a", "b"], ["c", "d"], ["e"]]


def test_extract_single_event():
    event_data = {
        "id": "4397712120",
        "type": "WatchEvent",
        "actor": {
            "id": 89842553,
            "url": "https://api.github.com/users/arsenydubrovin",
            "login": "arsenydubrovin",
            "display_login": "arsenydubrovin",
            "gravatar_id": "",
            "avatar_url": "https://avatars.githubusercontent.com/u/89842553?",
        },
        "repo": {
            "id": 197081291,
            "name": "iced-rs/iced",
            "url": "https://api.github.com/repos/iced-rs/iced",
        },
        "payload": {"action": "started"},
        "public": True,
        "created_at": "2025-11-01T00:00:00Z",
        "org": {
            "id": 54513237,
            "login": "iced-rs",
            "gravatar_id": "",
            "url": "https://api.github.com/orgs/iced-rs",
            "avatar_url": "https://avatars.githubusercontent.com/u/54513237?",
        },
    }

    repos, actors = extract_repos_and_actors([event_data])

    assert repos == ["iced-rs/iced"]
    assert actors == ["arsenydubrovin"]


def test_extract_actor_with_slash():
    event_data = {"repo": {"name": "user/repo1"}, "actor": {"login": "actor/bot"}}

    repos, actors = extract_repos_and_actors([event_data])

    assert repos == ["user/repo1"]
    assert actors == ["actor"]
