
CREATE TABLE IF NOT EXISTS ggazers.silver.fact_commit_comment_events (
    type STRING,
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    comment STRING
) USING ICEBERG
PARTITIONED BY (days(created_at))
