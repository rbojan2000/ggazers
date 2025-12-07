CREATE TABLE IF NOT EXISTS ggazers.silver.fact_release_events (
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    target_commitish STRING,
    tag_name STRING
) USING ICEBERG
PARTITIONED BY (days(created_at))
