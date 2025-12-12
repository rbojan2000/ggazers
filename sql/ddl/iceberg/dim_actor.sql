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
