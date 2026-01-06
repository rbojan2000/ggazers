CREATE TABLE repo_kpi (
    name_with_owner      TEXT,
    name                TEXT,
    owner               TEXT,
    commits_count        BIGINT,
    contributors_count   BIGINT,
    best_contributor_login TEXT,
    committers_map       JSONB,
    ggazer_score         DOUBLE PRECISION,
    window_start_utc     TEXT,
    window_end_utc       TEXT,
    PRIMARY KEY (name_with_owner, window_start_utc)
);