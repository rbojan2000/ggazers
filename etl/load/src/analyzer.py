from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class Analyzer:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def calculate_repo_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:

        df = self.spark_session.sql(
            f"""
            WITH repos_info AS (
                SELECT
                    DATE('{period_start_date}') AS period_start_date,
                    DATE('{period_end_date}')   AS period_end_date,
                    r.name,
                    r.owner,
                    COALESCE(r.description, 'N/A') AS description,
                    r.is_archived,
                    r.is_fork,
                    r.stargazers_count,
                    r.forks_count,
                    r.watchers_count,
                    r.issues_count,
                    r.primary_language,
                    repository_topics
                FROM
                    ggazers.silver.dim_repo r
            ),
            best_streak AS (
                SELECT
                    pe.repo_name,
                    COUNT(*)
                FROM
                    ggazers.silver.fact_push_events pe
                JOIN
                    ggazers.silver.fact_pull_request_events pre
                ON
                    pe.repo_name = pre.repo_name
                JOIN
                    ggazers.silver.fact_issue_events ie
                ON
                    pe.repo_name = ie.repo_name
                WHERE
                    pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}') AND
                    pre.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}') AND
                    ie.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                GROUP BY
                    pe.repo_name
            ),
            coding_sessions_num AS (
                SELECT
                    exploded_repo AS repo_name,
                    COUNT(*) AS sessions_count
                FROM
                    ggazers.silver.dim_coding_session
                LATERAL VIEW explode(split(repos, ',')) AS exploded_repo
                WHERE
                    session_start BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                GROUP BY
                    exploded_repo
            )

            """
        )

        return df
