import unittest

from paths import DATA_PATH
from pyspark.sql import SparkSession
from src.analyzer import Analyzer


class AnalyzerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[*]")
            .appName("AnalyzerTest")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
            .config(
                "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ggazers.type", "hadoop")
            .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.spark.sql("CREATE NAMESPACE IF NOT EXISTS ggazers.silver")

        # Create all tables upfront
        cls._create_tables(cls.spark)

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            tables = [
                "dim_repo",
                "dim_actor",
                "dim_coding_session",
                "fact_push_events",
                "fact_pull_request_events",
                "fact_commit_comment_events",
                "fact_create_events",
                "fact_discussion_events",
                "fact_fork_events",
                "fact_gollum_events",
                "fact_issue_comment_events",
                "fact_issue_events",
                "fact_member_events",
                "fact_pull_request_review_comment_events",
                "fact_release_events",
                "fact_watch_events",
            ]
            for table in tables:
                cls.spark.sql(f"DROP TABLE IF EXISTS ggazers.silver.{table}")
            cls.spark.sql("DROP NAMESPACE IF EXISTS ggazers.silver")
            cls.spark.stop()

    @classmethod
    def _create_tables(cls, spark):
        """Create all Iceberg tables needed for tests."""
        spark.sql(
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
                ingested_at TIMESTAMP,
                primary_language STRING,
                repository_topics STRING,
                updated_at TIMESTAMP
            ) USING ICEBERG
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.dim_actor (
                type STRING,
                login STRING NOT NULL,
                avatar_url STRING,
                name STRING,
                email STRING,
                website_url STRING,
                description STRING,
                company STRING,
                location STRING,
                created_at TIMESTAMP,
                followers_count BIGINT,
                following_count BIGINT,
                repositories_count BIGINT,
                gists_count BIGINT,
                twitter_username STRING,
                status_message STRING,
                updated_at TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (type)
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.dim_coding_session (
                actor_login STRING NOT NULL,
                session_start TIMESTAMP NOT NULL,
                session_end TIMESTAMP NOT NULL,
                duration BIGINT NOT NULL,
                event_count INT NOT NULL,
                repos STRING NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (days(session_start))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_push_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                ref STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_pull_request_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                action STRING,
                number STRING,
                assignees ARRAY<STRING>,
                labels ARRAY<STRING>
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_issue_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                title STRING,
                action STRING,
                labels ARRAY<STRING>,
                assignees ARRAY<STRING>
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_create_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                ref_type STRING,
                ref STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_commit_comment_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                comment STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_issue_comment_events (
                type STRING,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                created_at TIMESTAMP NOT NULL,
                title STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_fork_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                forked_repo_name STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_gollum_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                page_titles ARRAY<STRING>
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_member_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL,
                member STRING
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS ggazers.silver.fact_watch_events (
                type STRING,
                created_at TIMESTAMP NOT NULL,
                actor_login STRING NOT NULL,
                repo_name STRING NOT NULL
            ) USING ICEBERG
            PARTITIONED BY (days(created_at))
        """
        )

        for table in [
            "fact_discussion_events",
            "fact_pull_request_review_comment_events",
            "fact_release_events",
        ]:
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS ggazers.silver.{table} (
                    type STRING,
                    created_at TIMESTAMP NOT NULL,
                    actor_login STRING NOT NULL,
                    repo_name STRING NOT NULL
                ) USING ICEBERG
                PARTITIONED BY (days(created_at))
            """
            )

    def setUp(self):
        self.analyzer = Analyzer(self.spark)
        for table in [
            "dim_repo",
            "dim_actor",
            "dim_coding_session",
            "fact_push_events",
            "fact_pull_request_events",
            "fact_commit_comment_events",
            "fact_create_events",
            "fact_discussion_events",
            "fact_fork_events",
            "fact_gollum_events",
            "fact_issue_comment_events",
            "fact_issue_events",
            "fact_member_events",
            "fact_pull_request_review_comment_events",
            "fact_release_events",
            "fact_watch_events",
        ]:
            self.spark.sql(f"DELETE FROM ggazers.silver.{table}")
        self._setup_test_data()

    def tearDown(self):
        pass

    def _setup_test_data(self):
        """Insert test data into Iceberg tables."""

        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_repo VALUES (
                'test-owner/test-repo', 'test-repo', 'test-owner', 'A test repository',
                false, false, false, 1024, 'PUBLIC',
                100, 20, 50, 10, NULL,
                'Python', 'testing,python', CURRENT_TIMESTAMP()
            )
        """
        )

        self.spark.sql(
            """
            INSERT INTO ggazers.silver.fact_push_events VALUES
            (
                'PushEvent', CAST('2025-11-01 10:00:00' AS TIMESTAMP),
                'user1', 'test-owner/test-repo', 'refs/heads/main'
            ),
            (
                'PushEvent', CAST('2025-11-01 11:00:00' AS TIMESTAMP),
                'user2', 'test-owner/test-repo', 'refs/heads/main'
            ),
            (
                'PushEvent', CAST('2025-11-02 10:00:00' AS TIMESTAMP),
                'user1', 'test-owner/test-repo', 'refs/heads/main'
            )
        """
        )

        self.spark.sql(
            """
            INSERT INTO ggazers.silver.fact_pull_request_events VALUES
            (
                'PullRequestEvent', CAST('2025-11-01 12:00:00' AS TIMESTAMP),
                'user1', 'test-owner/test-repo', 'opened', '1', NULL, NULL
            ),
            (
                'PullRequestEvent', CAST('2025-11-02 12:00:00' AS TIMESTAMP),
                'user2', 'test-owner/test-repo', 'closed', '1', NULL, NULL
            )
        """
        )

        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_coding_session VALUES
            ('user1', CAST('2025-11-01 10:00:00' AS TIMESTAMP),
             CAST('2025-11-01 12:00:00' AS TIMESTAMP), 7200, 2, 'test-owner/test-repo')
        """
        )

        self.spark.sql(
            """
            INSERT INTO ggazers.silver.dim_actor VALUES
            ('User', 'user1', NULL, 'User One', NULL, NULL, NULL, 'Company A', NULL,
             CAST('2020-01-01 00:00:00' AS TIMESTAMP), 100, 50, 25, 10, NULL, NULL, CURRENT_TIMESTAMP()),
            ('User', 'user2', NULL, 'User Two', NULL, NULL, NULL, 'Company B', NULL,
             CAST('2020-01-02 00:00:00' AS TIMESTAMP), 150, 75, 30, 15, NULL, NULL, CURRENT_TIMESTAMP())
        """
        )

    def test_calculate_repo_level_stats(self):
        """Test the calculate_repo_level_stats method."""
        result_df = self.analyzer.calculate_repo_level_stats("2025-11-01", "2025-11-03")
        results = result_df.collect()

        row = results[0]

        self.assertEqual(len(results), 1, "Should return one repo")
        self.assertEqual(row.name, "test-repo")
        self.assertEqual(row.owner, "test-owner")
        self.assertEqual(row.name_with_owner, "test-owner/test-repo")
        self.assertEqual(row.description, "A test repository")
        self.assertEqual(row.primary_language, "Python")

        # Activity metrics
        self.assertEqual(row.best_streak, 2, "Should have 2 days of activity")
        self.assertEqual(row.coding_sessions_count, 1, "Should have 1 coding session")
        self.assertEqual(row.commiters_count, 2, "Should have 2 committers")

        # Ggazer score
        self.assertGreater(row.ggazer_score, 0, "Ggazer score should be calculated")
        self.assertIn(row.activity_label, ["Highly Active", "Active", "Moderate", "Low", "Inactive"])
        self.assertGreater(row.ggazer_rank, 0, "Ggazer rank should be assigned")

        # Period dates
        self.assertEqual(str(row.period_start_date), "2025-11-01")
        self.assertEqual(str(row.period_end_date), "2025-11-03")

    def test_calculate_repo_level_stats_empty_period(self):
        result_df = self.analyzer.calculate_repo_level_stats("2025-10-01", "2025-10-02")
        results = result_df.collect()

        # Should still return repo info but with zero activity
        self.assertEqual(len(results), 1)

        row = results[0]
        self.assertEqual(row.name_with_owner, "test-owner/test-repo")
        self.assertEqual(row.best_streak, 0)
        self.assertEqual(row.coding_sessions_count, 0)
        self.assertEqual(row.commiters_count, 0)
