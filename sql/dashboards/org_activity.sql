WITH last_period_date AS (
  SELECT
    MAX(period_start_date) AS last_period
  FROM org_level_stats
)
SELECT count(activity_label) as cnt, activity_label
FROM org_level_stats ols
JOIN last_period_date lpd
  ON ols.period_start_date = lpd.last_period
GROUP BY activity_label
ORDER BY cnt