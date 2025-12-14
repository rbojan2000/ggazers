CREATE TABLE IF NOT EXISTS user_level_stats (
    period_start_date DATE NOT NULL,
    period_end_date DATE NOT NULL,
    login VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    avatar_url TEXT,
    email VARCHAR(255),
    website_url TEXT,
    description TEXT,
    company VARCHAR(255),
    location VARCHAR(255),
    branches_count BIGINT DEFAULT 0,
    tags_count BIGINT DEFAULT 0,
    coding_sessions_count BIGINT DEFAULT 0,
    most_sessions_repo VARCHAR(500) DEFAULT 'N/A',
    most_sessions_repo_sessions_count BIGINT DEFAULT 0,
    commits_count BIGINT DEFAULT 0,
    most_commited_organization VARCHAR(255) DEFAULT 'N/A',
    longest_coding_session_seconds BIGINT,
    opened_pull_requests_count BIGINT DEFAULT 0,
    ggazers_score NUMERIC(10, 2) DEFAULT 0.0,
    activity_label VARCHAR(50) DEFAULT 'Inactive',
    ggazer_rank BIGINT DEFAULT 0,
    PRIMARY KEY (login, period_start_date)
);

COMMENT ON TABLE user_level_stats IS 'Aggregated user-level statistics for a specific period.';