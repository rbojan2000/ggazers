SELECT DATE_TRUNC('hour', window_end_utc) AS window_end_utc, name_with_owner AS name_with_owner, sum(commits_count) AS "SUM(commits_count)" 
FROM (SELECT
    name_with_owner,
    name,
    owner,
    commits_count,
    contributors_count,
    best_contributor_login,
    committers_map,
    ggazer_score,
    window_start_utc::timestamp AS window_start_utc,
    window_end_utc::timestamp   AS window_end_utc
FROM repo_kpi
) AS virtual_table JOIN (SELECT name_with_owner AS name_with_owner__, sum(commits_count) AS mme_inner__ 
FROM (SELECT
    name_with_owner,
    name,
    owner,
    commits_count,
    contributors_count,
    best_contributor_login,
    committers_map,
    ggazer_score,
    window_start_utc::timestamp AS window_start_utc,
    window_end_utc::timestamp   AS window_end_utc
FROM repo_kpi
) AS virtual_table GROUP BY name_with_owner ORDER BY sum(commits_count) DESC 
 LIMIT 5) AS series_limit ON name_with_owner = name_with_owner__ GROUP BY DATE_TRUNC('hour', window_end_utc), name_with_owner ORDER BY "SUM(commits_count)" DESC 
 LIMIT 1000;