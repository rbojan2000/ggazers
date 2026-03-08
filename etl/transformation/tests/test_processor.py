import json
import shutil
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator, List
from unittest.mock import patch

import pytest
from src.processor import Processor
from src.transformer import Transformer


class TestProcessor:
    @pytest.fixture(autouse=True)
    def setup(self, spark_session, silver_tables: Any) -> Generator[None, Any, None]:
        self.spark = spark_session
        transformer = Transformer(session_gap_seconds=8 * 60 * 60)
        self.processor = Processor(transformer)
        self.processor.spark_session = self.spark
        self.test_data_dir = Path(tempfile.mkdtemp())
        self.actors_dir = self.test_data_dir / "actors"
        self.repos_dir = self.test_data_dir / "repos"
        self.actors_dir.mkdir(parents=True)
        self.repos_dir.mkdir(parents=True)
        yield
        if self.test_data_dir.exists():
            shutil.rmtree(self.test_data_dir)

    def _create_actor_test_file(self, date_str: str, part: int, data: List[Dict[str, Any]]) -> str:
        date_dir = self.actors_dir / date_str
        date_dir.mkdir(exist_ok=True)
        file_path = date_dir / f"{date_str}_{part}.jsonl"
        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")
        return str(file_path)

    def _create_repo_test_file(self, date_str: str, part: int, data: List[Dict[str, Any]]) -> str:
        date_dir = self.repos_dir / date_str
        date_dir.mkdir(exist_ok=True)
        file_path = date_dir / f"{date_str}_{part}.jsonl"
        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")
        return str(file_path)

    @patch("src.processor.build_paths")
    def test_process_actors_insert(self, mock_build_paths: Any):
        """Test inserting new actors"""
        test_data = [
            {
                "__typename": "User",
                "id": "U_kgDOCg9nVQ",
                "login": "octocat",
                "avatarUrl": "https://avatars.githubusercontent.com/u/168781653?v=4",
                "name": "",
                "email": "",
                "bio": "",
                "company": "",
                "location": "",
                "websiteUrl": "",
                "createdAt": "2024-05-02T20:55:42Z",
                "twitterUsername": "",
                "followers": {"totalCount": 100},
                "following": {"totalCount": 0},
                "repositories": {"totalCount": 1},
                "gists": {"totalCount": 0},
                "status": {"message": "", "emoji": ":astronaut:"},
                "ingested_at": 1764290997,
            }
        ]
        file_path = self._create_actor_test_file("2025_11_01", 0, test_data)
        mock_build_paths.return_value = [file_path]
        self.processor.process_actors(date(2025, 11, 1), date(2025, 11, 1))
        result = self.spark.sql("SELECT * FROM ggazers.silver.dim_actor").collect()
        assert len(result) == 1
        assert result[0].login == "octocat"
        assert result[0].followers_count == 100

    @patch("src.processor.build_paths")
    def test_process_actors_update(self, mock_build_paths: Any) -> None:
        """Test updating existing actors"""
        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_actor VALUES (
                'User', 'octocat', 'https://old.url', NULL, NULL, NULL,
                NULL, NULL, NULL, CAST('2020-01-01' AS TIMESTAMP),
                50, 25, 10, 5, NULL, NULL, CURRENT_TIMESTAMP()
            )
            """
        )

        test_data = [
            {
                "id": "123",
                "login": "octocat",
                "__typename": "User",
                "avatarUrl": "https://new.url",
                "websiteUrl": "https://website.url",
                "createdAt": "2020-01-01T00:00:00Z",
                "twitterUsername": "octocat",
                "followers": {"totalCount": 150},
                "following": {"totalCount": 75},
                "repositories": {"totalCount": 30},
                "gists": {"totalCount": 15},
                "status": {"message": "Updated status"},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]

        file_path = self._create_actor_test_file("2025_11_01", 0, test_data)
        mock_build_paths.return_value = [file_path]

        self.processor.process_actors(date(2025, 11, 1), date(2025, 11, 1))

        result = self.spark.sql("SELECT * FROM ggazers.silver.dim_actor").collect()
        assert len(result) == 1
        assert result[0].login == "octocat"
        assert result[0].avatar_url == "https://new.url"
        assert result[0].followers_count == 150

    @patch("src.processor.build_paths")
    def test_process_repos_insert(self, mock_build_paths: Any) -> None:
        """Test inserting new repositories"""
        test_data = [
            {
                "nameWithOwner": "octocat/hello-world",
                "description": "My first repository",
                "createdAt": "2020-01-01T00:00:00Z",
                "isPrivate": False,
                "isArchived": False,
                "isFork": False,
                "diskUsage": 1024,
                "visibility": "PUBLIC",
                "stargazerCount": 100,
                "forkCount": 10,
                "watchers": {"totalCount": 50},
                "issues": {"totalCount": 5},
                "primaryLanguage": {"name": "Python"},
                "repositoryTopics": {
                    "nodes": [
                        {"topic": {"name": "python"}},
                        {"topic": {"name": "testing"}},
                    ]
                },
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]

        file_path = self._create_repo_test_file("2025_11_01", 0, test_data)
        mock_build_paths.return_value = [file_path]

        self.processor.process_repos(date(2025, 11, 1), date(2025, 11, 1))

        result = self.spark.sql("SELECT * FROM ggazers.silver.dim_repo").collect()
        assert len(result) == 1
        assert result[0].name_with_owner == "octocat/hello-world"
        assert result[0].owner == "octocat"
        assert result[0].name == "hello-world"
        assert result[0].stargazers_count == 100

    def test_process_repos_update(self) -> None:
        """Test updating existing repositories"""
        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_repo VALUES (
                'octocat/hello-world', 'hello-world', 'octocat', 'Old description',
                FALSE, FALSE, FALSE, 512, 'PUBLIC', 50, 5, 25, 2,
                'JavaScript', 'javascript,web', CURRENT_TIMESTAMP()
            )
            """
        )

        test_data = [
            {
                "nameWithOwner": "octocat/hello-world",
                "description": "Updated repository description",
                "createdAt": "2020-01-01T00:00:00Z",
                "isPrivate": False,
                "isArchived": False,
                "isFork": False,
                "diskUsage": 2048,
                "visibility": "PUBLIC",
                "stargazerCount": 150,
                "forkCount": 15,
                "watchers": {"totalCount": 75},
                "issues": {"totalCount": 10},
                "primaryLanguage": {"name": "Python"},
                "repositoryTopics": {
                    "nodes": [
                        {"topic": {"name": "python"}},
                        {"topic": {"name": "data-engineering"}},
                    ]
                },
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]

        file_path = self._create_repo_test_file("2025_11_01", 0, test_data)

        with patch("src.processor.build_paths", return_value=[file_path]):
            self.processor.process_repos(date(2025, 11, 1), date(2025, 11, 1))

        result = self.spark.sql("SELECT * FROM ggazers.silver.dim_repo").collect()
        assert len(result) == 1
        assert result[0].description == "Updated repository description"
        assert result[0].disk_usage == 2048
        assert result[0].stargazers_count == 150
