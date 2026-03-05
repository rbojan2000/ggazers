SELECT medal AS medal, name_with_owner AS name_with_owner, commits_count AS commits_count, contributors_count AS contributors_count, best_contributor_login AS best_contributor_login, window_start_utc AS window_start_utc, window_end_utc AS window_end_utc 
FROM (WITH ranked AS (
    SELECT
        name_with_owner,
        commits_count,
        contributors_count,
        best_contributor_login,
        ggazer_score,
        window_start_utc,
        window_end_utc,
        ROW_NUMBER() OVER (ORDER BY ggazer_score DESC) AS position
    FROM repo_kpi
    WHERE window_start_utc = (
        SELECT MAX(window_start_utc) FROM repo_kpi
    )
)
SELECT
    CASE
        WHEN position = 1 THEN '🥇'
        WHEN position = 2 THEN '🥈'
        WHEN position = 3 THEN '🥉'
        ELSE ''
    END AS medal,
    name_with_owner,
    commits_count,
    contributors_count,
    best_contributor_login,
    window_start_utc,
    window_end_utc
FROM ranked
WHERE position <= 10
ORDER BY position
) AS virtual_table 
 LIMIT 1000;