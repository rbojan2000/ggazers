import http
from datetime import date
from unittest.mock import MagicMock, patch

import requests
from git_clients.gh_archive_client import GHArchiveClient


def make_response(status_code=http.HTTPStatus.OK, content=b"DATA"):
    mock = MagicMock()
    mock.status_code = status_code
    mock.content = content
    return mock


def test_format_date():
    client = GHArchiveClient()
    assert client._format_date(date(2025, 1, 2)) == "2025-01-02"


@patch("requests.get")
def test_fetch_data_success(mock_get):
    mock_get.return_value = make_response(http.HTTPStatus.OK, b"OK")

    client = GHArchiveClient()
    result = client._fetch_data(date(2025, 1, 1), 0)

    assert result == b"OK"
    mock_get.assert_called_once()


@patch("requests.get")
def test_fetch_data_http_error(mock_get):
    mock_get.return_value = make_response(http.HTTPStatus.NOT_FOUND, b"")

    client = GHArchiveClient()
    result = client._fetch_data(date(2025, 1, 1), 10)

    assert result == b""


@patch("requests.get")
def test_fetch_data_exception(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException("Network fail")

    client = GHArchiveClient()
    result = client._fetch_data(date(2025, 1, 1), 0)

    assert result == b""


@patch.object(GHArchiveClient, "_fetch_data")
def test_fetch_for_date(mock_fetch):
    # Simulate parts: ok, empty, ok
    mock_fetch.side_effect = [b"A", b"", b"B"]

    client = GHArchiveClient()
    result = list(client._fetch_for_date(date(2025, 1, 1), parts_per_date=3))

    assert result == [
        (b"A", 0),
        (b"B", 2),
    ]


@patch.object(GHArchiveClient, "_fetch_for_date")
def test_get_events_dump(mock_fetch_for_date):
    # For each date return two "parts"
    mock_fetch_for_date.side_effect = [
        [(b"X1", 0), (b"X2", 1)],  # for day 1
        [(b"Y1", 0)],  # for day 2
    ]

    client = GHArchiveClient()
    start = date(2025, 1, 1)
    end = date(2025, 1, 2)

    results = list(client.get_events_dump(start, end))

    assert results == [
        (b"X1", date(2025, 1, 1), 0),
        (b"X2", date(2025, 1, 1), 1),
        (b"Y1", date(2025, 1, 2), 0),
    ]
    assert mock_fetch_for_date.call_count == 2
