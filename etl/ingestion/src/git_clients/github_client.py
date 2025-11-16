import logging
import os
import re
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)


class GithubClient:
    def __init__(
        self,
        graphql_api_url: str = "https://api.github.com/graphql",
        content_type: str = "application/json",
        accept: str = "application/vnd.github.v4+json",
        token: str = os.getenv("GIT_TOKEN"),
    ) -> None:
        self.api_url = graphql_api_url
        self.headers = self._init_headers(content_type, accept, token)

    def _init_headers(self, content_type: str, accept: str, token: str) -> Dict[str, str]:
        headers = {
            "Authorization": f"token {token}",
            "Content-Type": content_type,
            "Accept": accept,
        }

        return headers

    def build_query(self, actors: List[str] = None, repos: List[str] = None) -> str:
        logger.debug(f"actors: {actors}, repos: {repos}")

        query = """
        {
        """
        if actors:
            for _, actor in enumerate(actors):
                query += f"""
                {self._sanitize_field_name(actor)}: user(login: "{actor}") {{
                    id
                    login
                    name
                    email
                    bio
                    avatarUrl
                    url
                    websiteUrl
                    location
                    company
                    createdAt
                    followers {{ totalCount }}
                    following {{ totalCount }}
                    repositories {{ totalCount }}
                    gists {{ totalCount }}
                    starredRepositories {{ totalCount }}
                    watching {{ totalCount }}
                    twitterUsername
                    status {{
                        message
                        emoji
                    }}
                }}
                """
        if repos:
            for _, repo in enumerate(repos):
                owner, name = repo.split("/")
                query += f"""
                {self._sanitize_field_name(repo)}: repository(owner: "{owner}", name: "{name}") {{
                    name
                    description
                    stargazerCount
                    forkCount
                }}
                """
        query += """
        }
        """
        return query

    def run_query(self, query: str) -> Dict[str, Dict[str, Any]]:
        response = requests.post(
            url=self.api_url,
            json={"query": query},
            headers=self.headers,
            stream=False,
            timeout=60 * 5,
        )
        logger.debug(f"Response Status Code: {response.status_code}")
        logger.debug(f"Response Content: {response.content}")

        result = response.json().get("data")
        return result

    def _sanitize_field_name(self, name: str) -> str:
        """Sanitize field name for GraphQL."""
        if not name:
            return "field"

        # Replace all invalid characters with underscores
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)

        # Ensure it starts with a letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = f"_{sanitized}"

        # Remove consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)

        # Remove trailing underscores
        sanitized = sanitized.strip("_")

        return sanitized or "field"
