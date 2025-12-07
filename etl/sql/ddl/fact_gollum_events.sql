CREATE TABLE IF NOT EXISTS ggazers.silver.fact_gollum_events (
    created_at TIMESTAMP NOT NULL,
    actor_login STRING NOT NULL,
    repo_name STRING NOT NULL,
    page_titles ARRAY<STRING>
) USING ICEBERG
PARTITIONED BY (days(created_at))
