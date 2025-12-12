from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

COUNT_SCHEMA = StructType([StructField("totalCount", IntegerType(), True)])

STATUS_SCHEMA = StructType(
    [
        StructField("message", StringType(), True),
        StructField("emoji", StringType(), True),
    ]
)

ACTORS_SCHEMA = StructType(
    [
        StructField("__typename", StringType(), False),
        StructField("id", StringType(), False),
        StructField("login", StringType(), False),
        StructField("ingested_at", LongType(), False),
        StructField("avatarUrl", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("websiteUrl", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("company", StringType(), True),
        StructField("location", StringType(), True),
        StructField("twitterUsername", StringType(), True),
        StructField("createdAt", TimestampType(), True),
        StructField("followers", COUNT_SCHEMA, True),
        StructField("following", COUNT_SCHEMA, True),
        StructField("repositories", COUNT_SCHEMA, True),
        StructField("gists", COUNT_SCHEMA, True),
        StructField("status", STATUS_SCHEMA, True),
    ]
)

TOPIC_NODE_SCHEMA = StructType(
    [StructField("topic", StructType([StructField("name", StringType(), True)]), True)]
)

REPOS_SCHEMA = StructType(
    [
        StructField("nameWithOwner", StringType(), False),
        StructField("description", StringType(), True),
        StructField("createdAt", StringType(), False),
        StructField("isPrivate", BooleanType(), False),
        StructField("isArchived", BooleanType(), False),
        StructField("isFork", BooleanType(), False),
        StructField("diskUsage", IntegerType(), True),
        StructField("visibility", StringType(), False),
        StructField("stargazerCount", IntegerType(), False),
        StructField("forkCount", IntegerType(), False),
        StructField("watchers", StructType([StructField("totalCount", IntegerType(), False)]), False),
        StructField("issues", StructType([StructField("totalCount", IntegerType(), False)]), False),
        StructField("primaryLanguage", StructType([StructField("name", StringType(), True)]), True),
        StructField(
            "repositoryTopics",
            StructType([StructField("nodes", ArrayType(TOPIC_NODE_SCHEMA), True)]),
            True,
        ),
        StructField("ingested_at", StringType(), False),
    ]
)

ACTOR_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("login", StringType(), False),
        StructField("display_login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

REPO_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
        StructField("url", StringType(), True),
    ]
)

ORG_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

GITHUB_EVENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("type", StringType(), False),
        StructField("actor", ACTOR_SCHEMA, False),
        StructField("repo", REPO_SCHEMA, False),
        StructField("payload", StringType(), True),
        StructField("public", BooleanType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("org", ORG_SCHEMA, True),
        StructField("ingested_at", LongType(), False),
    ]
)
