import json
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from src.processor import DataProcessor


class DataProcessorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse_path = Path(cls.temp_dir) / "warehouse"
        cls.warehouse_path.mkdir()

        cls.spark = (
            SparkSession.builder.master("local[*]")
            .appName("DataProcessorTest")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.warehouse.dir", str(cls.warehouse_path))
            .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ggazers.type", "hadoop")
            .config("spark.sql.catalog.ggazers.warehouse", str(cls.warehouse_path))
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS ggazers.silver")

        cls.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.dim_actor (
                login STRING NOT NULL,
                type STRING,
                avatar_url STRING,
                website_url STRING,
                created_at TIMESTAMP,
                twitter_username STRING,
                followers_count BIGINT,
                following_count BIGINT,
                repositories_count BIGINT,
                gists_count BIGINT,
                status_message STRING,
                updated_at TIMESTAMP
            ) USING ICEBERG
            """
        )

        cls.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.dim_repo (
                name_with_owner STRING NOT NULL,
                name STRING,
                owner STRING,
                description STRING,
                is_private BOOLEAN,
                is_archived BOOLEAN,
                is_fork BOOLEAN,
                disk_usage BIGINT,
                visibility STRING,
                stargazers_count BIGINT,
                forks_count BIGINT,
                watchers_count BIGINT,
                issues_count BIGINT,
                primary_language STRING,
                repository_topics STRING,
                updated_at TIMESTAMP
            ) USING ICEBERG
            """
        )

        cls.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.dim_coding_session (
                actor_login STRING NOT NULL,
                session_start TIMESTAMP NOT NULL,
                session_end TIMESTAMP,
                duration BIGINT,
                event_count BIGINT
            ) USING ICEBERG
            """
        )

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.sql("DROP TABLE IF EXISTS ggazers.silver.dim_actor")
            cls.spark.sql("DROP TABLE IF EXISTS ggazers.silver.dim_repo")
            cls.spark.sql("DROP TABLE IF EXISTS ggazers.silver.dim_coding_session")
            cls.spark.sql("DROP NAMESPACE IF EXISTS ggazers.silver")
            cls.spark.stop()
            import time

            time.sleep(1)

        if Path(cls.temp_dir).exists():
            shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.spark.sql("DELETE FROM ggazers.silver.dim_actor")
        self.spark.sql("DELETE FROM ggazers.silver.dim_repo")
        self.spark.sql("DELETE FROM ggazers.silver.dim_coding_session")

        self.processor = DataProcessor(self.spark)

        self.test_data_dir = Path(tempfile.mkdtemp())
        self.actors_dir = self.test_data_dir / "actors"
        self.repos_dir = self.test_data_dir / "repos"
        self.actors_dir.mkdir(parents=True)
        self.repos_dir.mkdir(parents=True)

    def tearDown(self):
        if self.test_data_dir.exists():
            shutil.rmtree(self.test_data_dir)

    def _create_actor_test_file(self, date_str: str, part: int, data: list):
        date_dir = self.actors_dir / date_str
        date_dir.mkdir(exist_ok=True)
        file_path = date_dir / f"{date_str}_{part}.jsonl"

        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        return str(file_path)

    def _create_repo_test_file(self, date_str: str, part: int, data: list):
        date_dir = self.repos_dir / date_str
        date_dir.mkdir(exist_ok=True)
        file_path = date_dir / f"{date_str}_{part}.jsonl"

        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        return str(file_path)

    @patch("src.processor.build_paths")
    def test_process_actors_insert(self, mock_build_paths):
        """Test inserting new actors"""
        test_data = [
            {
                "id": "123",
                "login": "octocat",
                "__typename": "User",
                "avatarUrl": "https://avatar.url",
                "websiteUrl": "https://website.url",
                "createdAt": "2020-01-01T00:00:00Z",
                "twitterUsername": "octocat",
                "followers": {"totalCount": 100},
                "following": {"totalCount": 50},
                "repositories": {"totalCount": 25},
                "gists": {"totalCount": 10},
                "status": {"message": "Working on projects"},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]

        file_path = self._create_actor_test_file("2025_11_01", 0, test_data)
        mock_build_paths.return_value = [file_path]

        self.processor.process_actors(date(2025, 11, 1), date(2025, 11, 1))

        result = self.spark.sql("SELECT * FROM ggazers.silver.dim_actor").collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].login, "octocat")
        self.assertEqual(result[0].followers_count, 100)

    @patch("src.processor.build_paths")
    def test_process_actors_update(self, mock_build_paths):
        """Test updating existing actors"""
        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_actor VALUES (
                'octocat', 'User', 'https://old.url', NULL,
                CAST('2020-01-01' AS TIMESTAMP), NULL,
                50, 25, 10, 5, NULL, CURRENT_TIMESTAMP()
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
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].login, "octocat")
        self.assertEqual(result[0].avatar_url, "https://new.url")
        self.assertEqual(result[0].followers_count, 150)

    @patch("src.processor.build_paths")
    def test_process_repos_insert(self, mock_build_paths):
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
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name_with_owner, "octocat/hello-world")
        self.assertEqual(result[0].owner, "octocat")
        self.assertEqual(result[0].name, "hello-world")
        self.assertEqual(result[0].stargazers_count, 100)

    def test_process_repos_update(self):
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
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].description, "Updated repository description")
        self.assertEqual(result[0].disk_usage, 2048)
        self.assertEqual(result[0].stargazers_count, 150)
