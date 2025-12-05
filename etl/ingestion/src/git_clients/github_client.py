import http
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


class GithubClient:
    def __init__(
        self,
        graphql_api_url: str = "https://api.github.com/graphql",
        http_rest_api_url: str = "https://api.github.com",
        content_type: str = "application/json",
        accept: str = "application/vnd.github.v4+json",
        primary_token: Optional[str] = os.getenv("GIT_TOKEN_1"),
        secondary_token: Optional[str] = os.getenv("GIT_TOKEN_2"),
        timeout: int = 60 * 10,
    ) -> None:
        self.api_url = graphql_api_url
        self.content_type = content_type
        self.accept = accept
        self.http_rest_api_url = http_rest_api_url
        self.primary_token = primary_token
        self.secondary_token = secondary_token
        self.timeout = timeout

    def _headers(self, token: Optional[str]) -> Dict[str, str]:
        headers = {
            "Content-Type": self.content_type,
            "Accept": self.accept,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _attempts(self) -> List[Tuple[str, Optional[str]]]:
        attempts = [
            ("primary token", self.primary_token),
            ("secondary token", self.secondary_token),
            ("no token", None),
        ]
        return attempts

    def run_query(self, query: str) -> Optional[List[Any]]:
        """
        Execute query using multiple auth strategies (primary → secondary → no token).
        Returns response data if available, otherwise {}.
        """

        for label, token in self._attempts():
            logger.info(f"Trying query using {label}...")

            try:
                response = requests.post(
                    url=self.api_url,
                    json={"query": query},
                    headers=self._headers(token),
                    timeout=self.timeout,
                )
            except Exception as e:
                logger.warning(f"Request failed using {label}: {e}")
                continue

            try:
                body = response.json()
            except ValueError:
                logger.warning(f"Invalid JSON using {label}: {response.text}")
                continue

            if "err" in body:
                logger.warning(f"API error using {label}: {body}")
                continue

            data = body.get("data")
            if data:
                data = list(data.values())
                logger.info(f"Query successful using {label}.")
                return data

            logger.warning(f"Query returned no data using {label}: {body}")

        logger.warning("All attempts failed. Returning empty result.")
        return None

    def build_graphql_query(
        self, actors: Optional[List[str]] = None, repos: Optional[List[str]] = None
    ) -> Optional[str]:
        if not actors and not repos:
            logger.warning("No actors or repositories provided for query building.")
            return None

        query = "{"

        if actors:
            for actor in actors:
                field = self._sanitize_field_name(actor)
                query += f"""
                {field}: repositoryOwner(login: "{actor}") {{
                    __typename
                    id
                    login
                    avatarUrl

                    ... on User {{
                        name
                        email
                        bio
                        company
                        location
                        websiteUrl
                        createdAt
                        twitterUsername
                        followers {{
                            totalCount
                        }}
                        following {{
                            totalCount
                        }}
                        repositories {{
                            totalCount
                        }}
                        gists {{
                            totalCount
                        }}
                        status {{
                            message
                            emoji
                        }}
                    }}

                    ... on Organization {{
                        login
                        id
                        name
                        avatarUrl
                        websiteUrl
                        location
                        email
                        twitterUsername
                        createdAt
                        repositories {{
                            totalCount
                        }}
                    }}
                }}
                """

        if repos:
            for repo in repos:
                owner, name = repo.split("/")
                field = self._sanitize_field_name(repo)
                query += f"""
                {field}: repository(owner: "{owner}", name: "{name}") {{
                    id
                    nameWithOwner
                    description
                    createdAt
                    isPrivate
                    isArchived
                    isFork
                    diskUsage
                    visibility
                    stargazerCount
                    forkCount
                    watchers {{
                        totalCount
                    }}
                    issues(states: OPEN) {{
                        totalCount
                    }}
                    primaryLanguage {{
                        name
                    }}
                    repositoryTopics(first: 10) {{
                        nodes {{
                            topic {{
                                name
                            }}
                        }}
                    }}
                }}
                """

        query += "}"
        return query

    def send_http_request(self, endpoint: str, unit: str) -> Optional[Dict[str, Any]]:
        for attempt, token in self._attempts():
            logger.info(f"GET {self.http_rest_api_url} using {attempt}...")
            try:
                response = requests.get(
                    url=f"{self.http_rest_api_url}/{endpoint}/{unit}",
                    headers=self._headers(token),
                    timeout=self.timeout,
                )

                if response.status_code == http.HTTPStatus.OK:
                    return response.json()
                elif response.status_code == http.HTTPStatus.NOT_FOUND:
                    logger.warning(f"{attempt}, {endpoint}/{unit} not found.")
                    return None

                logger.warning(f"Failed with {attempt}: HTTP {response.status_code} → trying next")

            except Exception as e:
                logger.warning(f"GET request failed using {attempt}: {e}")

        logger.warning("All attempts failed for GET request.")
        return None

    def hit_rest_api(self, endpoint: str, units: List[str]) -> List[Any]:
        data: List[Any] = []
        for unit in units:
            unit_data = self.send_http_request(endpoint, unit)
            data.append(unit_data)
        return None if not data else data

    def _sanitize_field_name(self, name: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9_]", "_", name)
        sanitized = re.sub(r"_+", "_", sanitized)
        if not sanitized:
            return "field"
        if sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized
