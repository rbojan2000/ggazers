CREATE TABLE IF NOT EXISTS ggazers.silver.dim_repo (
    name_with_owner STRING NOT NULL,
    name STRING,
    owner STRING,
    description STRING,
    is_private BOOLEAN,
    is_archived BOOLEAN,
    is_fork BOOLEAN,
    disk_usage BIGINT,
    visibility STRING,
    stargazers_count BIGINT,
    forks_count BIGINT,
    watchers_count BIGINT,
    issues_count BIGINT,
    ingested_at TIMESTAMP,
    primary_language STRING,
    repository_topics STRING,
    updated_at TIMESTAMP
) USING ICEBERG
