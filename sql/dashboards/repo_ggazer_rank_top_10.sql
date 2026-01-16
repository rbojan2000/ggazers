WITH last_period_date AS (
  SELECT 
    MAX(period_start_date) AS last_period
  FROM repo_level_stats
)
SELECT DISTINCT
  CASE
    WHEN rls.ggazer_rank = 1 THEN '🥇'
    WHEN rls.ggazer_rank = 2 THEN '🥈'
    WHEN rls.ggazer_rank = 3 THEN '🥉'
    ELSE ''
  END AS medal,
  rls.ggazer_rank,
  TO_CHAR(rls.period_start_date, 'YYYY-MM') AS period_month,
  rls.name,
  rls.owner,
  rls.description,
  rls.primary_language,
  rls.activity_label,
  rls.ggazer_score
FROM repo_level_stats rls
JOIN last_period_date lpd
  ON rls.period_start_date = lpd.last_period
ORDER BY rls.ggazer_rank ASC
LIMIT 10;
