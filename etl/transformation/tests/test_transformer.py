from datetime import datetime

import pytest
from pyspark.sql.functions import lit, when
from src.schema import ACTORS_SCHEMA, GITHUB_EVENTS_SCHEMA, REPOS_SCHEMA
from src.transformer import Transformer


class TestTransformer:
    @pytest.fixture(autouse=True)
    def setup(self, spark_session):
        self.spark = spark_session
        self.transformer = Transformer()

    def test_flatten_repository_topics_multiple_topics(self) -> None:
        """Test flattening repository topics with multiple topics"""
        data = [
            {
                "nameWithOwner": "octocat/hello-world",
                "description": "Test repo",
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
                "repositoryTopics": {"nodes": [{"topic": {"name": "python"}}, {"topic": {"name": "spark"}}]},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]
        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df = df.withColumnRenamed("nameWithOwner", "name_with_owner")
        df_transformed = self.transformer._flatten_repository_topics(df)
        result = df_transformed.collect()[0]
        assert result.name_with_owner == "octocat/hello-world"
        assert result.repository_topics in ["python,spark", "spark,python"]

    def test_flatten_repository_topics_no_topics(self) -> None:
        """Test flattening repository topics with no topics"""
        data = [
            {
                "nameWithOwner": "user/repo",
                "description": "Test repo",
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
                "repositoryTopics": {"nodes": []},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]
        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df = df.withColumnRenamed("nameWithOwner", "name_with_owner")
        df_transformed = self.transformer._flatten_repository_topics(df)
        result = df_transformed.select("repository_topics").collect()[0].repository_topics
        assert result is None

    def test_flatten_repository_topics_single_topic(self) -> None:
        """Test flattening repository topics with single topic"""
        data = [
            {
                "nameWithOwner": "user/repo",
                "description": "Test repo",
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
                "repositoryTopics": {"nodes": [{"topic": {"name": "javascript"}}]},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]
        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df = df.withColumnRenamed("nameWithOwner", "name_with_owner")
        df_transformed = self.transformer._flatten_repository_topics(df)
        result = df_transformed.select("repository_topics").collect()[0].repository_topics
        assert result == "javascript"

    def test_transform_actors_deduplication(self) -> None:
        """Test that actors are deduplicated by most recent ingested_at"""
        data = [
            {
                "__typename": "User",
                "ingested_at": 1730419200000,
                "id": "123",
                "login": "octocat",
                "avatarUrl": "https://old.url",
                "name": "The Octocat",
                "email": None,
                "bio": None,
                "company": None,
                "location": None,
                "websiteUrl": None,
                "twitterUsername": None,
                "createdAt": datetime(2020, 1, 1, 0, 0, 0),
                "followers": {"totalCount": 100},
                "following": {"totalCount": 50},
                "repositories": {"totalCount": 25},
                "gists": {"totalCount": 10},
                "status": None,
            },
            {
                "__typename": "User",
                "ingested_at": 1730422800000,
                "id": "123",
                "login": "octocat",
                "avatarUrl": "https://new.url",
                "name": "The Octocat",
                "email": None,
                "bio": None,
                "company": None,
                "location": None,
                "websiteUrl": None,
                "twitterUsername": None,
                "createdAt": datetime(2020, 1, 1, 0, 0, 0),
                "followers": {"totalCount": 150},
                "following": {"totalCount": 75},
                "repositories": {"totalCount": 30},
                "gists": {"totalCount": 15},
                "status": None,
            },
        ]

        df = self.spark.createDataFrame(data, ACTORS_SCHEMA)
        df_transformed = self.transformer.transform_actors(df)
        result = df_transformed.collect()

        # Should only have 1 record (most recent)
        assert len(result) == 1
        assert result[0].avatar_url == "https://new.url"
        assert result[0].followers_count == 150

    def test_transform_actors_column_mapping(self) -> None:
        """Test that actor columns are properly renamed and transformed"""
        data = [
            {
                "__typename": "User",
                "ingested_at": 1730419200000,
                "id": "123",
                "login": "octocat",
                "avatarUrl": "https://avatar.url",
                "name": "The Octocat",
                "email": "octocat@github.com",
                "bio": "I code",
                "company": "GitHub",
                "location": "San Francisco",
                "websiteUrl": "https://website.url",
                "twitterUsername": "octocat",
                "createdAt": datetime(2020, 1, 1, 0, 0, 0),
                "followers": {"totalCount": 100},
                "following": {"totalCount": 50},
                "repositories": {"totalCount": 25},
                "gists": {"totalCount": 10},
                "status": {"message": "Coding", "emoji": "💻"},
            }
        ]

        df = self.spark.createDataFrame(data, ACTORS_SCHEMA)
        df_transformed = self.transformer.transform_actors(df)
        result = df_transformed.collect()[0]

        assert result.type == "User"
        assert result.avatar_url == "https://avatar.url"
        assert result.website_url == "https://website.url"
        assert result.twitter_username == "octocat"
        assert result.followers_count == 100
        assert result.following_count == 50
        assert result.repositories_count == 25
        assert result.gists_count == 10
        assert result.status_message == "Coding"
        assert "id" not in df_transformed.columns
        assert "followers" not in df_transformed.columns
        assert "following" not in df_transformed.columns
        assert "ingested_at" not in df_transformed.columns

    def test_transform_actors_with_nulls(self) -> None:
        """Test transforming actors with null values"""
        data = [
            {
                "__typename": "User",
                "ingested_at": 1730419200000,
                "id": "123",
                "login": "octocat",
                "avatarUrl": None,
                "name": "The Octocat",
                "email": None,
                "bio": None,
                "company": None,
                "location": None,
                "websiteUrl": None,
                "twitterUsername": None,
                "createdAt": datetime(2020, 1, 1, 0, 0, 0),
                "followers": {"totalCount": 100},
                "following": {"totalCount": 50},
                "repositories": {"totalCount": 25},
                "gists": {"totalCount": 10},
                "status": None,
            }
        ]

        df = self.spark.createDataFrame(data, ACTORS_SCHEMA)
        df_transformed = self.transformer.transform_actors(df)
        result = df_transformed.collect()[0]

        assert result.login == "octocat"
        assert result.avatar_url is None
        assert result.website_url is None
        assert result.twitter_username is None
        assert result.status_message is None

    def test_transform_repos_column_mapping(self) -> None:
        """Test that repo columns are properly renamed and transformed"""
        data = [
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

        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df_transformed = self.transformer.transform_repos(df)
        result = df_transformed.collect()[0]

        assert result.name_with_owner == "octocat/hello-world"
        assert result.owner == "octocat"
        assert result.name == "hello-world"
        assert result.is_private is False
        assert result.is_archived is False
        assert result.is_fork is False
        assert result.disk_usage == 1024
        assert result.stargazers_count == 100
        assert result.forks_count == 10
        assert result.watchers_count == 50
        assert result.issues_count == 5
        assert result.primary_language == "Python"
        assert result.repository_topics in ["python,testing", "testing,python"]
        assert "ingested_at" not in df_transformed.columns
        assert "watchers" not in df_transformed.columns
        assert "issues" not in df_transformed.columns

    def test_transform_repos_deduplication(self) -> None:
        """Test that repos are deduplicated by most recent ingested_at"""
        data = [
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
                "repositoryTopics": {"nodes": []},
                "ingested_at": "2025-11-01T00:00:00Z",
            },
            {
                "nameWithOwner": "octocat/hello-world",
                "description": "My first repository - updated",
                "createdAt": "2020-01-01T00:00:00Z",
                "isPrivate": False,
                "isArchived": False,
                "isFork": False,
                "diskUsage": 2048,
                "visibility": "PUBLIC",
                "stargazerCount": 200,
                "forkCount": 20,
                "watchers": {"totalCount": 100},
                "issues": {"totalCount": 10},
                "primaryLanguage": {"name": "Python"},
                "repositoryTopics": {"nodes": []},
                "ingested_at": "2025-11-02T00:00:00Z",
            },
        ]

        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df_transformed = self.transformer.transform_repos(df)
        result = df_transformed.collect()

        assert len(result) == 1
        assert result[0].stargazers_count == 200
        assert result[0].disk_usage == 2048

    def test_transform_repos_split_name_with_owner(self) -> None:
        """Test that name_with_owner is correctly split into owner and name"""
        data = [
            {
                "nameWithOwner": "microsoft/vscode",
                "description": "Visual Studio Code",
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
                "primaryLanguage": {"name": "TypeScript"},
                "repositoryTopics": {"nodes": []},
                "ingested_at": "2025-11-01T00:00:00Z",
            }
        ]

        df = self.spark.createDataFrame(data, REPOS_SCHEMA)
        df_transformed = self.transformer.transform_repos(df)
        result = df_transformed.collect()[0]

        assert result.name_with_owner == "microsoft/vscode"
        assert result.owner == "microsoft"
        assert result.name == "vscode"

    def test_transform_events_column_extraction(self):
        """Test that events are properly transformed and columns extracted"""
        data = [
            {
                "id": "event123",
                "type": "PushEvent",
                "actor": {
                    "id": 1,
                    "login": "octocat",
                    "display_login": "octocat",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/octocat",
                    "avatar_url": "https://avatars.githubusercontent.com/u/1?",
                },
                "repo": {
                    "id": 1,
                    "name": "octocat/Hello-World",
                    "url": "https://api.github.com/repos/octocat/Hello-World",
                },
                "created_at": datetime(2025, 11, 1, 12, 0, 0),
                "payload": {"push_id": "123", "size": "1"},
                "public": True,
                "org": None,
                "ingested_at": 1730462400000,
            }
        ]

        df = self.spark.createDataFrame(data, GITHUB_EVENTS_SCHEMA)
        df_transformed = self.transformer.transform_events(df)
        result = df_transformed.collect()[0]

        assert result.actor_login == "octocat"
        assert result.repo_name == "octocat/Hello-World"
        assert result.created_at == datetime(2025, 11, 1, 12, 0, 0)
        assert "id" not in df_transformed.columns
        assert "actor" not in df_transformed.columns
        assert "repo" not in df_transformed.columns
        assert "ingested_at" not in df_transformed.columns

    def test_transform_events_null_filtering(self) -> None:
        """Test that events with null required fields are filtered out"""

        data = [
            {
                "id": "event1",
                "type": "PushEvent",
                "actor": {
                    "id": 1,
                    "login": "octocat",
                    "display_login": "octocat",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/octocat",
                    "avatar_url": "https://avatars.githubusercontent.com/u/1?",
                },
                "repo": {
                    "id": 1,
                    "name": "octocat/Hello-World",
                    "url": "https://api.github.com/repos/octocat/Hello-World",
                },
                "created_at": datetime(2025, 11, 1, 12, 0, 0),
                "payload": {},
                "public": True,
                "org": None,
                "ingested_at": 1730462400000,
            },
            {
                "id": "event2",
                "type": "IssueCommentEvent",
                "actor": {
                    "id": 2,
                    "login": "octocat",
                    "display_login": "octocat",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/octocat",
                    "avatar_url": "https://avatars.githubusercontent.com/u/1?",
                },
                "repo": {
                    "id": 1,
                    "name": "octocat/Hello-World",
                    "url": "https://api.github.com/repos/octocat/Hello-World",
                },
                "created_at": datetime(2025, 11, 1, 12, 0, 0),
                "payload": {},
                "public": True,
                "org": None,
                "ingested_at": 1730462400000,
            },
        ]

        df = self.spark.createDataFrame(data, GITHUB_EVENTS_SCHEMA)
        # Simulate null by setting type to null for the second record
        df = df.withColumn("type", when(df.id == "event2", lit(None)).otherwise(df.type))

        df_transformed = self.transformer.transform_events(df)
        result = df_transformed.collect()

        # Only 1 event should remain (the valid one)
        assert len(result) == 1
        assert result[0].type == "PushEvent"

    def test_transform_sessions_single_session(self) -> None:
        """Test session transformation with events forming a single session"""
        data = [
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 10, 0, 0),
                "repo_name": "repo1",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 11, 0, 0),
                "repo_name": "repo2",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 13, 0, 0),
                "repo_name": "repo3",
            },
        ]

        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform_sessions(df)
        result = df_transformed.collect()

        assert len(result) == 1
        assert result[0].actor_login == "user1"
        assert result[0].events_count == 3
        assert result[0].session_duration_seconds == 3 * 60 * 60  # 3 hours

    def test_transform_sessions_multiple_sessions(self) -> None:
        """Test session transformation with events forming multiple sessions"""
        data = [
            # Session 1
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 10, 0, 0),
                "repo_name": "repo1",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 11, 0, 0),
                "repo_name": "repo2",
            },
            # Session 2 (9 hours later)
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 20, 0, 0),
                "repo_name": "repo3",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 21, 0, 0),
                "repo_name": "repo4",
            },
        ]

        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform_sessions(df)
        result = df_transformed.collect()

        assert len(result) == 2

    def test_transform_sessions_multiple_users(self) -> None:
        """Test session transformation with multiple users"""
        data = [
            # User 1
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 10, 0, 0),
                "repo_name": "repo1",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 11, 0, 0),
                "repo_name": "repo2",
            },
            # User 2
            {
                "actor_login": "user2",
                "created_at": datetime(2025, 11, 1, 10, 0, 0),
                "repo_name": "repo3",
            },
            {
                "actor_login": "user2",
                "created_at": datetime(2025, 11, 1, 12, 0, 0),
                "repo_name": "repo4",
            },
        ]

        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform_sessions(df)
        result = df_transformed.collect()

        assert len(result) == 2
        assert set([r.actor_login for r in result]) == {"user1", "user2"}

    def test_transform_sessions_exact_boundary(self) -> None:
        """Test session boundary at exactly 8 hours"""
        data = [
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 10, 0, 0),
                "repo_name": "repo1",
            },
            {
                "actor_login": "user1",
                "created_at": datetime(2025, 11, 1, 18, 0, 0),  # Exactly 8 hours later
                "repo_name": "repo2",
            },
        ]

        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform_sessions(df)
        result = df_transformed.collect()

        assert len(result) == 1
        assert result[0].events_count == 2
        assert result[0].session_duration_seconds == 8 * 60 * 60
        assert result[0].repos == "repo2,repo1"
