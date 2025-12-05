from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    collect_set,
    concat_ws,
    count,
    current_timestamp,
    explode,
    lag,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (
    row_number,
    split,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window


# fmt: off
class Transformer:
    def __init__(self):
        self.session_gap_seconds = 8 * 60 * 60  # 8 hours

    def _flatten_repository_topics(self, df: DataFrame) -> DataFrame:
        df_topics = df \
            .withColumn("topic_node", explode(col("repositoryTopics.nodes"))) \
            .withColumn("topic_name", col("topic_node.topic.name"))

        df_topics_agg = df_topics \
            .groupBy("name_with_owner") \
            .agg(
                concat_ws(",", collect_set("topic_name")).alias("repository_topics")
            )

        df_final = df \
            .drop("repositoryTopics") \
            .join(df_topics_agg, on="name_with_owner", how="left")

        return df_final

    def transform_actors(self, actors_df: DataFrame) -> DataFrame:
        actors_df = actors_df \
            .withColumnRenamed("__typename", "type") \
            .withColumnRenamed("avatarUrl", "avatar_url") \
            .withColumnRenamed("websiteUrl", "website_url") \
            .withColumnRenamed("createdAt", "created_at") \
            .withColumnRenamed("twitterUsername", "twitter_username") \
            .withColumn("followers_count", col("followers.totalCount")) \
            .withColumn("following_count", col("following.totalCount")) \
            .withColumn("repositories_count", col("repositories.totalCount")) \
            .withColumn("gists_count", col("gists.totalCount")) \
            .withColumn("status_message", col("status.message")) \
            .withColumn("updated_at", current_timestamp())

        window = Window \
            .partitionBy("login") \
            .orderBy(col("ingested_at").desc())

        actors_df = actors_df \
            .withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn", "followers", "following", "repositories", "gists", "status", "id", "ingested_at")

        return actors_df

    def transform_repos(self, repos_df: DataFrame) -> DataFrame:

        repos_df = repos_df \
            .withColumnRenamed("nameWithOwner", "name_with_owner") \
            .withColumnRenamed("isPrivate", "is_private") \
            .withColumnRenamed("isArchived", "is_archived") \
            .withColumnRenamed("isFork", "is_fork") \
            .withColumnRenamed("diskUsage", "disk_usage") \
            .withColumnRenamed("stargazerCount", "stargazers_count") \
            .withColumnRenamed("forkCount", "forks_count") \
            .withColumn("watchers_count", col("watchers.totalCount")) \
            .withColumn("issues_count", col("issues.totalCount")) \
            .withColumn("primary_language", col("primaryLanguage.name")) \
            .withColumn("updated_at", current_timestamp()) \
            .transform(self._flatten_repository_topics)

        repos_df = repos_df \
            .withColumn("owner", split(col("name_with_owner"), "/").getItem(0)) \
            .withColumn("name", split(col("name_with_owner"), "/").getItem(1))

        window = Window \
            .partitionBy("name_with_owner") \
            .orderBy(col("ingested_at").desc())

        repos_df = repos_df \
            .withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn", "ingested_at", "watchers", "issues", "primaryLanguage", "createdAt")

        return repos_df

    def transform_sessions(self, events_df: DataFrame) -> DataFrame:

        window_spec = Window \
            .partitionBy("actor_login") \
            .orderBy("event_time")

        df_with_sessions = (
            events_df.select(
                col("actor_login"),
                col("event_time"),
            )
            .withColumn("prev_event_time", lag("event_time").over(window_spec))
            .withColumn(
                "time_diff_seconds",
                unix_timestamp("event_time") - unix_timestamp("prev_event_time"),
            )
            .withColumn(
                "is_new_session",
                when(
                    col("prev_event_time").isNull() | (col("time_diff_seconds") > self.session_gap_seconds),
                    1
                ).otherwise(0),
            )
        )

        window_session_id = Window \
            .partitionBy("actor_login") \
            .orderBy("event_time") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        df_with_session_ids = df_with_sessions \
            .withColumn("session_id", spark_sum("is_new_session").over(window_session_id))

        df_sessions = (
            df_with_session_ids
            .groupBy("actor_login", "session_id")
            .agg(
                spark_min("event_time").alias("session_start"),
                spark_max("event_time").alias("session_end"),
                (
                    unix_timestamp(spark_max("event_time")) - unix_timestamp(spark_min("event_time"))
                ).alias("session_duration_seconds"),
                count("*").alias("events_count"),
            )
            .drop("session_id")
            .filter(col("session_duration_seconds") > 0)
            .withColumn("updated_at", current_timestamp())
        )
        return df_sessions

    def transform_commit_comment_events(self, events_df: DataFrame) -> DataFrame:
        commit_comment_events_df = events_df \
            .filter(col("type") == "CommitCommentEvent") \
            .withColumn("comment", col("payload").getItem("comment").getItem("body")) \

        return commit_comment_events_df

    def transform_events(self, events_df: DataFrame) -> DataFrame:
        events_df = events_df \
            .dropna(subset=["type", "actor.login", "repo.name", "created_at"]) \
            .withColumn("actor_login", col("actor.login")) \
            .withColumn("repo_name", col("repo.name")) \
            .withColumnRenamed("created_at", "event_time") \
            .drop("id", "ingested_at", "actor", "repo", "org")

        return events_df

# fmt: on
