CREATE TABLE IF NOT EXISTS ggazers.silver.fact_pull_request_events (
    type STRING,
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    action STRING,
    number STRING,
    assignees ARRAY<STRING>,
    labels ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (days(created_at))