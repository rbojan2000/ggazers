CREATE TABLE IF NOT EXISTS ggazers.silver.fact_discussion_events (
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    title STRING,
    state STRING,
    comments_count STRING,
    locked BOOLEAN
) USING ICEBERG
PARTITIONED BY (days(created_at))
