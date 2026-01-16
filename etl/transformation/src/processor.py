import logging
from datetime import date
from typing import List

from pyspark.sql import DataFrame, SparkSession
from src.paths import DATA_PATH
from src.schema import ACTORS_SCHEMA, GITHUB_EVENTS_SCHEMA, REPOS_SCHEMA
from src.transformer import Transformer
from src.utils import build_paths

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self):
        self.transformer = Transformer()

    def init_spark_session(self) -> None:
        self.spark_session: SparkSession = (
            SparkSession.builder.appName("transformation")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
            .config(
                "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.shuffle.partitions", "5")
            .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ggazers.type", "hadoop")
            .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
            .getOrCreate()
        )

    def terminate_spark_session(self) -> None:
        self.spark_session.stop()

    def process_actors(self, start_date: date, end_date: date) -> None:
        paths: List[str] = build_paths(start_date=start_date, end_date=end_date, dataset="actors")

        actors_df = (
            self.spark_session.read.schema(ACTORS_SCHEMA)
            .json(paths)
            .transform(self.transformer.transform_actors)
        )
        actors_df.createOrReplaceTempView("staging_actor")

        self.spark_session.sql(
            """
                MERGE INTO
                    ggazers.silver.dim_actor AS target
                USING
                    staging_actor AS source
                ON
                    target.login = source.login
                WHEN MATCHED THEN
                    UPDATE SET
                        type            = source.type,
                        login           = target.login,
                        avatar_url      = source.avatar_url,
                        name            = source.name,
                        email           = source.email,
                        website_url     = source.website_url,
                        description     = source.description,
                        location        = source.location,
                        company         = source.company,
                        created_at      = source.created_at,
                        twitter_username = source.twitter_username,
                        followers_count = source.followers_count,
                        following_count = source.following_count,
                        repositories_count = source.repositories_count,
                        gists_count     = source.gists_count,
                        status_message  = source.status_message,
                        updated_at      = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (
                        login, name, type, email, description, location,
                        company, avatar_url, website_url, created_at,
                        twitter_username, followers_count, following_count,
                        repositories_count, gists_count, status_message, updated_at
                    )
                    VALUES (
                        source.login, source.name, source.type, source.email,
                        source.description, source.location, source.company,
                        source.avatar_url, source.website_url, source.created_at,
                        source.twitter_username, source.followers_count, source.following_count,
                        source.repositories_count, source.gists_count, source.status_message, source.updated_at
                    )
            """
        )

    def process_repos(self, start_date: date, end_date: date) -> None:
        paths: List[str] = build_paths(start_date=start_date, end_date=end_date, dataset="repos")

        repos_df = (
            self.spark_session.read.schema(REPOS_SCHEMA)
            .json(paths)
            .transform(self.transformer.transform_repos)
        )

        repos_df.createOrReplaceTempView("staging_repo")

        self.spark_session.sql(
            """
                MERGE INTO
                    ggazers.silver.dim_repo AS target
                USING
                    staging_repo AS source
                ON
                    target.name_with_owner = source.name_with_owner
                WHEN MATCHED THEN
                    UPDATE SET
                        name_with_owner    = target.name_with_owner,
                        name               = source.name,
                        owner              = source.owner,
                        description        = source.description,
                        is_private         = source.is_private,
                        is_archived       = source.is_archived,
                        is_fork           = source.is_fork,
                        disk_usage        = source.disk_usage,
                        visibility        = source.visibility,
                        stargazers_count  = source.stargazers_count,
                        forks_count       = source.forks_count,
                        watchers_count    = source.watchers_count,
                        issues_count      = source.issues_count,
                        primary_language  = source.primary_language,
                        repository_topics  = source.repository_topics,
                        updated_at        = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (
                        name_with_owner, name, owner, description, is_private, is_archived,
                        is_fork, disk_usage, visibility, stargazers_count, forks_count,
                        watchers_count, issues_count, primary_language, repository_topics, updated_at
                    )
                    VALUES (
                        source.name_with_owner, source.name, source.owner, source.description,
                        source.is_private, source.is_archived, source.is_fork,
                        source.disk_usage, source.visibility, source.stargazers_count,
                        source.forks_count, source.watchers_count, source.issues_count,
                        source.primary_language, source.repository_topics, source.updated_at
                    )
            """
        )

    def _process_events(self, events_df: DataFrame) -> None:
        fact_commit_comment_events_df = events_df.transform(self.transformer.transform_commit_comment_events)
        fact_create_events_df = events_df.transform(self.transformer.transform_create_events)
        fact_discussion_events_df = events_df.transform(self.transformer.transform_discussion_events)
        fact_fork_events_df = events_df.transform(self.transformer.transform_fork_events)
        fact_gollum_events_df = events_df.transform(self.transformer.transform_gollum_events)
        fact_issue_comment_events_df = events_df.transform(self.transformer.transform_issue_comment_events)
        fact_issue_events_df = events_df.transform(self.transformer.transform_issue_events)
        fact_member_events_df = events_df.transform(self.transformer.transform_member_events)
        fact_pull_request_events_df = events_df.transform(self.transformer.transform_pull_request_events)
        fact_pull_request_review_comment_events_df = events_df.transform(
            self.transformer.transform_pull_request_review_comment_events
        )
        fact_push_events_df = events_df.transform(self.transformer.transform_push_events)
        fact_release_events_df = events_df.transform(self.transformer.transform_release_events)
        fact_watch_events_df = events_df.transform(self.transformer.transform_watch_events)

        event_mappings = [
            (
                fact_commit_comment_events_df,
                "fact_commit_comment_events",
                ["actor_login", "repo_name", "created_at", "type"],
            ),
            (
                fact_create_events_df,
                "fact_create_events",
                ["actor_login", "repo_name", "created_at", "ref_type", "ref", "type"],
            ),
            (
                fact_discussion_events_df,
                "fact_discussion_events",
                ["actor_login", "repo_name", "created_at", "title", "type"],
            ),
            (fact_fork_events_df, "fact_fork_events", ["actor_login", "repo_name", "created_at", "type"]),
            (
                fact_gollum_events_df,
                "fact_gollum_events",
                ["actor_login", "repo_name", "created_at", "page_titles", "type"],
            ),
            (
                fact_issue_comment_events_df,
                "fact_issue_comment_events",
                ["actor_login", "repo_name", "created_at", "title", "type"],
            ),
            (
                fact_issue_events_df,
                "fact_issue_events",
                ["actor_login", "repo_name", "created_at", "title", "type"],
            ),
            (
                fact_member_events_df,
                "fact_member_events",
                ["actor_login", "repo_name", "created_at", "member", "type"],
            ),
            (
                fact_pull_request_events_df,
                "fact_pull_request_events",
                ["actor_login", "repo_name", "created_at", "number", "type"],
            ),
            (
                fact_pull_request_review_comment_events_df,
                "fact_pull_request_review_comment_events",
                ["actor_login", "repo_name", "created_at", "number", "comment", "type"],
            ),
            (fact_push_events_df, "fact_push_events", ["actor_login", "repo_name", "created_at", "ref"]),
            (fact_release_events_df, "fact_release_events", ["actor_login", "repo_name", "created_at"]),
            (fact_watch_events_df, "fact_watch_events", ["actor_login", "repo_name", "created_at"]),
        ]

        for df, table, merge_keys in event_mappings:
            logger.info(f"Appending to table: {table}")
            df.createOrReplaceTempView("staging_events")

            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            columns = ", ".join(df.columns)
            values = ", ".join([f"source.{col}" for col in df.columns])

            self.spark_session.sql(
                f"""
                    MERGE INTO
                        ggazers.silver.{table} AS target
                    USING
                        staging_events AS source
                    ON
                        {merge_condition}
                    WHEN NOT MATCHED THEN
                        INSERT (
                            {columns}
                        )
                        VALUES (
                            {values}
                        )
                """
            )

    def _process_sessions(self, events_df: DataFrame) -> None:
        sessions_df = self.transformer.transform_sessions(events_df)
        sessions_df.createOrReplaceTempView("staging_sessions")

        self.spark_session.sql(
            """
                MERGE INTO
                    ggazers.silver.dim_coding_session AS target
                USING
                    staging_sessions AS source
                ON
                    target.actor_login = source.actor_login
                    AND target.session_start = source.session_start
                WHEN NOT MATCHED THEN
                    INSERT (
                        actor_login, session_start, session_end, duration, event_count, repos
                    )
                    VALUES (
                        source.actor_login, source.session_start, source.session_end,
                        source.session_duration_seconds, source.events_count, source.repos
                    )
            """
        )

    def process_github_events(self, start_date: date, end_date: date) -> None:
        paths: List[str] = build_paths(start_date=start_date, end_date=end_date, dataset="github_events")

        events_df = (
            self.spark_session.read.schema(GITHUB_EVENTS_SCHEMA)
            .json(paths)
            .transform(self.transformer.transform_events)
        )

        self._process_sessions(events_df)
        self._process_events(events_df)
