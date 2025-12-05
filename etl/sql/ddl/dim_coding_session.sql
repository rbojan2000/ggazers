CREATE TABLE IF NOT EXISTS ggazers.silver.dim_coding_session (
    actor_login STRING NOT NULL,
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP NOT NULL,
    duration BIGINT NOT NULL,
    event_count INT NOT NULL
) USING ICEBERG
PARTITIONED BY (days(session_start))
