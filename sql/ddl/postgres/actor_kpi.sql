CREATE TABLE actor_kpi (
    actor_login VARCHAR(255),
    name VARCHAR(255),
    email VARCHAR(255),
    commits_count BIGINT,
    repos_contributed_to_count BIGINT,
    most_contributed_repo_name VARCHAR(255),
    repos_map JSON,
    ggazer_score DOUBLE PRECISION,
    window_start_utc VARCHAR(255),
    window_end_utc VARCHAR(255),
    PRIMARY KEY (actor_login, window_start_utc)
);