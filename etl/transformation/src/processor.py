from datetime import date
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    row_number,
    split,
)
from pyspark.sql.window import Window
from src.schema import ACTORS_SCHEMA, REPOS_SCHEMA
from src.transformer import Transformer
from src.utils import build_paths


class DataProcessor:

    @classmethod
    def process_actors(cls, spark: SparkSession, start_date: date, end_date: date) -> None:

        paths: List[str] = build_paths(start_date=start_date, end_date=end_date, dataset="actors")

        df = (
            spark.read.schema(ACTORS_SCHEMA)
            .json(paths)
            .dropDuplicates()
            .dropna(subset=["login"])
            .withColumnRenamed("__typename", "type")
            .withColumnRenamed("avatarUrl", "avatar_url")
            .withColumnRenamed("websiteUrl", "website_url")
            .withColumnRenamed("createdAt", "created_at")
            .withColumnRenamed("twitterUsername", "twitter_username")
            .withColumn("followers_count", col("followers.totalCount"))
            .withColumn("following_count", col("following.totalCount"))
            .withColumn("repositories_count", col("repositories.totalCount"))
            .withColumn("gists_count", col("gists.totalCount"))
            .withColumn("status_message", col("status.message"))
            .drop("followers", "following", "repositories", "gists", "status", "id")
            .withColumn("updated_at", current_timestamp())
        )

        window = Window.partitionBy("login").orderBy(col("ingested_at").desc())
        df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)

        df = df.drop("rn", "followers", "following", "repositories", "gists", "status", "id")
        df.createOrReplaceTempView("staging_actor")

        spark.sql(
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

    @classmethod
    def process_repos(cls, spark: SparkSession, start_date: date, end_date: date) -> None:
        paths: List[str] = build_paths(start_date=start_date, end_date=end_date, dataset="repos")

        df = (
            spark.read.schema(REPOS_SCHEMA)
            .json(paths)
            .dropDuplicates()
            .dropna(subset=["nameWithOwner"])
            .withColumnRenamed("nameWithOwner", "name_with_owner")
            .withColumn("owner", split(col("name_with_owner"), "/").getItem(0))
            .withColumn("name", split(col("name_with_owner"), "/").getItem(1))
            .withColumnRenamed("createdAt", "created_at")
            .withColumnRenamed("isPrivate", "is_private")
            .withColumnRenamed("isArchived", "is_archived")
            .withColumnRenamed("isFork", "is_fork")
            .withColumnRenamed("diskUsage", "disk_usage")
            .withColumnRenamed("stargazerCount", "stargazers_count")
            .withColumnRenamed("forkCount", "forks_count")
            .withColumn("watchers_count", col("watchers.totalCount"))
            .withColumn("issues_count", col("issues.totalCount"))
            .withColumn("primary_language", col("primaryLanguage.name"))
            .withColumn("updated_at", current_timestamp())
            .transform(Transformer.flatten_repository_topics)
        )

        window = Window.partitionBy("name_with_owner").orderBy(col("ingested_at").desc())
        df = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
        df = df.drop("rn", "ingested_at", "id", "watchers", "issues", "primaryLanguage")

        df.createOrReplaceTempView("staging_repo")

        spark.sql(
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
