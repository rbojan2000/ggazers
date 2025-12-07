CREATE TABLE IF NOT EXISTS ggazers.silver.fact_watch_events (
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL
) USING ICEBERG
PARTITIONED BY (days(created_at))
