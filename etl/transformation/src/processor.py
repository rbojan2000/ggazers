from datetime import date
from typing import List

from pyspark.sql import DataFrame, SparkSession
from src.schema import ACTORS_SCHEMA, GITHUB_EVENTS_SCHEMA, REPOS_SCHEMA
from src.transformer import Transformer
from src.utils import build_paths


class DataProcessor:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.transformer = Transformer()

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
                        login           = target.login,
                        type            = source.type,
                        avatar_url      = source.avatar_url,
                        website_url     = source.website_url,
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
                        login, type, avatar_url, website_url, created_at,
                        twitter_username, followers_count, following_count,
                        repositories_count, gists_count, status_message, updated_at
                    )
                    VALUES (
                        source.login, source.type, source.avatar_url, source.website_url,
                        source.created_at, source.twitter_username, source.followers_count,
                        source.following_count, source.repositories_count, source.gists_count,
                        source.status_message, source.updated_at
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
                        updated_at        = source.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (
                        name_with_owner, name, owner, description, is_private, is_archived,
                        is_fork, disk_usage, visibility, stargazers_count, forks_count,
                        watchers_count, issues_count, primary_language, updated_at
                    )
                    VALUES (
                        source.name_with_owner, source.name, source.owner, source.description,
                        source.is_private, source.is_archived, source.is_fork,
                        source.disk_usage, source.visibility, source.stargazers_count,
                        source.forks_count, source.watchers_count, source.issues_count,
                        source.primary_language, source.updated_at
                    )
            """
        )

    def _process_events(self, events_df: DataFrame) -> None:
        commit_comment_events = events_df.transform(self.transformer.transform_commit_comment_events)

        commit_comment_events.show(truncate=False, n=5)

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
                        actor_login, session_start, session_end, duration, event_count
                    )
                    VALUES (
                        source.actor_login, source.session_start, source.session_end,
                        source.session_duration_seconds, source.events_count
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
