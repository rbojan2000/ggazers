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
PARTITIONED BY (type)
