CREATE TABLE IF NOT EXISTS org_level_stats (
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
    most_active_repo VARCHAR(500) DEFAULT 'N/A',
    most_active_repo_activity_count BIGINT DEFAULT 0,
    commits_count BIGINT DEFAULT 0,
    most_active_member VARCHAR(255) DEFAULT 'N/A',
    most_active_member_activity_count BIGINT DEFAULT 0,
    new_stargazers_count BIGINT DEFAULT 0,
    total_repos_count BIGINT DEFAULT 0,
    ggazers_score NUMERIC(10, 2) DEFAULT 0.0,
    activity_label VARCHAR(50) DEFAULT 'Inactive',
    ggazer_rank BIGINT DEFAULT 0,
    PRIMARY KEY (login, period_start_date)
);

COMMENT ON TABLE org_level_stats IS 'Aggregated organization-level statistics for a specific period.';