import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when
from src.schema import ACTORS_SCHEMA, GITHUB_EVENTS_SCHEMA, REPOS_SCHEMA
from src.transformer import Transformer


class TransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[*]")
            .appName("TransformerTest")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    def setUp(self):
        self.transformer = Transformer()

    def test_flatten_repository_topics_multiple_topics(self):
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

        self.assertEqual(result.name_with_owner, "octocat/hello-world")
        # Topics are collected as a set, so order may vary
        self.assertIn(result.repository_topics, ["python,spark", "spark,python"])

    def test_flatten_repository_topics_no_topics(self):
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
        self.assertIsNone(result)

    def test_flatten_repository_topics_single_topic(self):
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
        self.assertEqual(result, "javascript")

    def test_transform_actors_deduplication(self):
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
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].avatar_url, "https://new.url")
        self.assertEqual(result[0].followers_count, 150)

    def test_transform_actors_column_mapping(self):
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

        self.assertEqual(result.type, "User")
        self.assertEqual(result.avatar_url, "https://avatar.url")
        self.assertEqual(result.website_url, "https://website.url")
        self.assertEqual(result.twitter_username, "octocat")
        self.assertEqual(result.followers_count, 100)
        self.assertEqual(result.following_count, 50)
        self.assertEqual(result.repositories_count, 25)
        self.assertEqual(result.gists_count, 10)
        self.assertEqual(result.status_message, "Coding")
        self.assertNotIn("id", df_transformed.columns)
        self.assertNotIn("followers", df_transformed.columns)
        self.assertNotIn("following", df_transformed.columns)
        self.assertNotIn("ingested_at", df_transformed.columns)

    def test_transform_actors_with_nulls(self):
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

        self.assertEqual(result.login, "octocat")
        self.assertIsNone(result.avatar_url)
        self.assertIsNone(result.website_url)
        self.assertIsNone(result.twitter_username)
        self.assertIsNone(result.status_message)

    def test_transform_repos_column_mapping(self):
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

        self.assertEqual(result.name_with_owner, "octocat/hello-world")
        self.assertEqual(result.owner, "octocat")
        self.assertEqual(result.name, "hello-world")
        self.assertEqual(result.is_private, False)
        self.assertEqual(result.is_archived, False)
        self.assertEqual(result.is_fork, False)
        self.assertEqual(result.disk_usage, 1024)
        self.assertEqual(result.stargazers_count, 100)
        self.assertEqual(result.forks_count, 10)
        self.assertEqual(result.watchers_count, 50)
        self.assertEqual(result.issues_count, 5)
        self.assertEqual(result.primary_language, "Python")
        self.assertIn(result.repository_topics, ["python,testing", "testing,python"])
        self.assertNotIn("ingested_at", df_transformed.columns)
        self.assertNotIn("watchers", df_transformed.columns)
        self.assertNotIn("issues", df_transformed.columns)

    def test_transform_repos_deduplication(self):
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

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].stargazers_count, 200)
        self.assertEqual(result[0].disk_usage, 2048)

    def test_transform_repos_split_name_with_owner(self):
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

        self.assertEqual(result.name_with_owner, "microsoft/vscode")
        self.assertEqual(result.owner, "microsoft")
        self.assertEqual(result.name, "vscode")

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

        self.assertEqual(result.actor_login, "octocat")
        self.assertEqual(result.repo_name, "octocat/Hello-World")
        self.assertEqual(result.created_at, datetime(2025, 11, 1, 12, 0, 0))
        self.assertNotIn("id", df_transformed.columns)
        self.assertNotIn("actor", df_transformed.columns)
        self.assertNotIn("repo", df_transformed.columns)
        self.assertNotIn("ingested_at", df_transformed.columns)

    def test_transform_events_null_filtering(self):
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
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].type, "PushEvent")

    def test_transform_sessions_single_session(self):
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

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].actor_login, "user1")
        self.assertEqual(result[0].events_count, 3)
        self.assertEqual(result[0].session_duration_seconds, 3 * 60 * 60)  # 3 hours

    def test_transform_sessions_multiple_sessions(self):
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

        self.assertEqual(len(result), 2)

    def test_transform_sessions_multiple_users(self):
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

        self.assertEqual(len(result), 2)
        self.assertSetEqual(set([r.actor_login for r in result]), {"user1", "user2"})

    def test_transform_sessions_exact_boundary(self):
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

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].events_count, 2)
        self.assertEqual(result[0].session_duration_seconds, 8 * 60 * 60)
        self.assertEqual(result[0].repos, "repo2,repo1")
