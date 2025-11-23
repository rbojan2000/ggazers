import http
from unittest.mock import MagicMock, patch

import pytest
import requests
from git_clients.github_client import GithubClient


def make_response(status=http.HTTPStatus.OK, json_data=None, text=""):
    mock = MagicMock()
    mock.status_code = status
    mock.text = text
    mock.json = MagicMock(return_value=json_data)
    return mock


def test_headers_with_token():
    client = GithubClient()
    hdr = client._headers("ABC")
    assert hdr["Authorization"] == "Bearer ABC"


def test_headers_no_token():
    client = GithubClient()
    hdr = client._headers(None)
    assert "Authorization" not in hdr


def test_attempts_order():
    client = GithubClient(primary_token="A", secondary_token="B")
    attempts = client._attempts()
    assert attempts == [
        ("primary token", "A"),
        ("secondary token", "B"),
        ("no token", None),
    ]


@patch("requests.post")
def test_run_query_success(mock_post):
    mock_post.return_value = make_response(http.HTTPStatus.OK, {"data": {"ok": True}})

    client = GithubClient(primary_token="TOKEN1")
    result = client.run_query("query { test }")

    assert result == {"ok": True}
    assert mock_post.call_count == 1


@patch("requests.post")
def test_run_query_primary_fails_secondary_ok(mock_post):
    mock_post.side_effect = [
        make_response(http.HTTPStatus.OK, {"err": "rate limit"}),  # primary token fails
        make_response(http.HTTPStatus.OK, {"data": {"x": 123}}),  # secondary succeeds
    ]

    client = GithubClient(primary_token="A", secondary_token="B")
    result = client.run_query("query { test }")

    assert result == {"x": 123}
    assert mock_post.call_count == 2


@patch("requests.post")
def test_run_query_invalid_json(mock_post):
    res = make_response(http.HTTPStatus.OK, None, text="bad json")
    res.json.side_effect = ValueError("json error")

    mock_post.return_value = res

    client = GithubClient()
    result = client.run_query("query { t }")

    assert result is None


@patch("requests.post")
def test_run_query_all_fail(mock_post):
    mock_post.side_effect = requests.exceptions.RequestException("fail")

    client = GithubClient()
    result = client.run_query("query { x }")

    assert result is None


def test_build_graphql_query_actors_and_repos():
    client = GithubClient()
    q = client.build_graphql_query(actors=["alice"], repos=["owner/repo"])
    assert 'user(login: "alice")' in q
    assert 'repository(owner: "owner", name: "repo")' in q


def test_build_graphql_query_none():
    client = GithubClient()
    assert client.build_graphql_query(None, None) is None


@patch("requests.get")
def test_send_http_request_ok(mock_get):
    mock_get.return_value = make_response(http.HTTPStatus.OK, {"value": 42})

    client = GithubClient()
    res = client.send_http_request("users", "octocat")

    assert res == {"value": 42}


@patch("requests.get")
def test_send_http_request_404(mock_get):
    mock_get.return_value = make_response(http.HTTPStatus.NOT_FOUND)

    client = GithubClient()
    res = client.send_http_request("users", "missing")

    assert res is None


@patch("requests.get")
def test_send_http_request_failover(mock_get):
    mock_get.side_effect = [
        make_response(http.HTTPStatus.INTERNAL_SERVER_ERROR),
        make_response(http.HTTPStatus.OK, {"ok": 1}),
    ]

    client = GithubClient(primary_token="A", secondary_token="B")
    res = client.send_http_request("repos", "unit")

    assert res == {"ok": 1}


@patch.object(GithubClient, "send_http_request")
def test_hit_rest_api(mock_send):
    mock_send.side_effect = [
        {"x": 1},
        None,
    ]

    client = GithubClient()
    result = client.hit_rest_api("users", ["a", "b"])

    assert result == {
        "a": {"x": 1},
        "b": None,
    }


@pytest.mark.parametrize(
    "name,expected",
    [
        ("validName", "validName"),
        ("invalid name", "invalid_name"),
        ("weird@@@name", "weird_name"),
        ("123abc", "_123abc"),
        ("", "field"),
    ],
)
def test_sanitize_field_name(name, expected):
    client = GithubClient()
    assert client._sanitize_field_name(name) == expected
