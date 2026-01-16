WITH last_period_date AS (
  SELECT 
    MAX(period_start_date) AS last_period
  FROM org_level_stats
)
SELECT
  CASE
    WHEN ols.ggazer_rank = 1 THEN '🥇'
    WHEN ols.ggazer_rank = 2 THEN '🥈'
    WHEN ols.ggazer_rank = 3 THEN '🥉'
    ELSE ''
  END AS medal,
  ols.ggazer_rank,
  TO_CHAR(ols.period_start_date, 'YYYY-MM') AS period_month,
  ols.name,
  ols.email,
  ols.activity_label,
  ols.ggazers_score AS ggazer_score
FROM org_level_stats ols
JOIN last_period_date lpd
  ON ols.period_start_date = lpd.last_period
ORDER BY ols.ggazer_rank
LIMIT 10;
