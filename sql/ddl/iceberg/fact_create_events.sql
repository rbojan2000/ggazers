CREATE TABLE IF NOT EXISTS ggazers.silver.fact_create_events (
    type STRING,
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    ref_type STRING,
    ref STRING
) USING ICEBERG
PARTITIONED BY (days(created_at))
