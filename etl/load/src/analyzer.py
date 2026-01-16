from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class Analyzer:
    def __init__(self, spark_session: SparkSession):
        self.spark_session: SparkSession = spark_session

    def calculate_repo_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:
        repo_level_stats = self.spark_session.sql(
            f"""
                WITH repos_info AS (
                    SELECT
                        r.name,
                        r.owner,
                        r.name_with_owner,
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
                        repo_name,
                        COUNT(DISTINCT activity_date) as best_streak
                    FROM (
                        SELECT
                            repo_name,
                            DATE(created_at) as activity_date
                        FROM
                            ggazers.silver.fact_push_events
                        WHERE
                            created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        UNION
                            SELECT
                                repo_name,
                                DATE(created_at) as activity_date
                            FROM
                                ggazers.silver.fact_pull_request_events
                            WHERE
                                created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    )
                    GROUP BY repo_name
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
                ),
                most_comments_actor AS (
                    SELECT
                        actor_login,
                        repo_name,
                        comments_count
                    FROM (
                        SELECT
                            ie.actor_login,
                            ie.repo_name,
                            COUNT(*) AS comments_count,
                            ROW_NUMBER() OVER (PARTITION BY ie.repo_name ORDER BY COUNT(*) DESC) AS rn
                        FROM
                            ggazers.silver.fact_commit_comment_events ie
                        WHERE
                            created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        GROUP BY
                            ie.repo_name, ie.actor_login
                    )
                    WHERE
                        rn = 1
                ),
                commiters_num AS (
                    SELECT
                        pe.repo_name,
                        COUNT(DISTINCT pe.actor_login) AS commiters_count
                    FROM
                        ggazers.silver.fact_push_events pe
                    WHERE
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pe.repo_name
                ),
                commiters_organizations_num AS (
                    SELECT
                        pe.repo_name,
                        COUNT(DISTINCT dim_actor.company) AS organizations_count
                    FROM
                        ggazers.silver.fact_push_events pe
                    JOIN
                        ggazers.silver.dim_actor AS dim_actor
                    ON
                        pe.actor_login = dim_actor.login
                    WHERE
                        dim_actor.company IS NOT NULL AND
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pe.repo_name
                    ORDER BY
                        organizations_count DESC
                ),
                new_branches_num AS (
                    SELECT
                        ce.repo_name,
                        COUNT(*) AS new_branches_count
                    FROM
                        ggazers.silver.fact_create_events ce
                    WHERE
                        ce.ref_type = 'branch' AND
                        ce.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        ce.repo_name
                ),
                new_tags_num AS (
                    SELECT
                        ce.repo_name,
                        COUNT(*) AS new_tags_count
                    FROM
                        ggazers.silver.fact_create_events ce
                    WHERE
                        ce.ref_type = 'tag' AND
                        ce.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        ce.repo_name
                ),
                issues_stats AS (
                    SELECT
                        repo_name,
                        opened_issues_count,
                        closed_issues_count,
                        CASE
                            WHEN opened_issues_count = 0 THEN 0
                            ELSE closed_issues_count / opened_issues_count
                        END AS resolved_issue_rate
                    FROM (
                        SELECT
                            ie.repo_name,
                            SUM (
                                CASE
                                    WHEN ie.action = 'opened' THEN 1
                                    ELSE 0
                                END
                            ) AS opened_issues_count,
                            SUM (
                                CASE
                                    WHEN ie.action = 'closed' THEN 1
                                    ELSE 0
                                END
                            ) AS closed_issues_count
                        FROM ggazers.silver.fact_issue_events ie
                        WHERE
                            ie.action IN ('opened', 'closed')
                            AND ie.created_at BETWEEN DATE('{period_start_date}')
                                                AND DATE('{period_end_date}')
                        GROUP BY ie.repo_name
                    ) t
                ),
                oldest_unresolved_issue AS (
                    SELECT
                        ie.repo_name,
                        MIN(ie.created_at) AS oldest_unresolved_issue_date
                    FROM
                        ggazers.silver.fact_issue_events ie
                    WHERE
                        ie.action = 'opened'
                    GROUP BY
                        ie.repo_name
                ),
                oldest_unresolved_issue_title AS (
                    SELECT
                        ouie.repo_name,
                        fi.title AS oldest_unresolved_issue_title
                    FROM
                        oldest_unresolved_issue ouie
                    JOIN
                        ggazers.silver.fact_issue_events fi
                    ON
                        ouie.repo_name = fi.repo_name AND
                        ouie.oldest_unresolved_issue_date = fi.created_at
                ),
                most_commented_issue AS (
                    SELECT
                        repo_name,
                        most_commented_issue_title,
                        comments_count
                    FROM (
                        SELECT
                            ice.repo_name,
                            ice.title AS most_commented_issue_title,
                            count(*) AS comments_count,
                            ROW_NUMBER() OVER (PARTITION BY ice.repo_name ORDER BY COUNT(*) DESC) AS rn
                        FROM
                            ggazers.silver.fact_issue_comment_events ice
                        GROUP BY
                            ice.repo_name, ice.title
                    ) ranked
                    WHERE rn = 1
                ),
                forks_num AS (
                    SELECT
                        ff.repo_name,
                        COUNT(*) AS forks_count
                    FROM
                        ggazers.silver.fact_fork_events ff
                    WHERE
                        ff.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        ff.repo_name
                ),
                docs_stats AS (
                    SELECT
                        repo_name,
                        COUNT(*) AS pages_updated_count,
                        CASE
                            WHEN pages_updated_count > 0 THEN 0
                            ELSE 1
                        END AS docs_updated_flag
                    FROM
                        ggazers.silver.fact_gollum_events pe
                    WHERE
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        repo_name
                ),
                new_members_num AS (
                    SELECT
                        me.repo_name,
                        COUNT(*) AS new_members_count
                    FROM
                        ggazers.silver.fact_member_events me
                    WHERE
                        me.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        me.repo_name
                ),
                pull_requests_stats AS (
                    SELECT
                        repo_name,
                        SUM(
                            CASE
                                WHEN action = 'opened' THEN pull_requests_count
                                ELSE 0
                        END
                        ) AS opened_pull_requests_count,
                        SUM(
                            CASE
                                WHEN action = 'closed' THEN pull_requests_count
                                ELSE 0
                        END
                        ) AS closed_pull_requests_count
                    FROM (
                        SELECT
                            pre.repo_name,
                            pre.action,
                            COUNT(*) AS pull_requests_count
                        FROM
                            ggazers.silver.fact_pull_request_events pre
                        WHERE
                            pre.action IN ('opened', 'closed')  AND
                            pre.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        GROUP BY
                            pre.repo_name, pre.action
                    )
                    GROUP BY
                        repo_name
                ),
                most_pull_recuest_actor AS(
                    SELECT
                        pre.actor_login,
                        pre.repo_name,
                        COUNT(*) AS pull_requests_count
                    FROM
                        ggazers.silver.fact_pull_request_events pre
                    WHERE
                        pre.action = 'opened' AND
                        pre.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pre.actor_login, pre.repo_name
                ),
                stargazers_num AS (
                    SELECT
                        se.repo_name,
                        COUNT(*) AS stargazers_count
                    FROM
                        ggazers.silver.fact_watch_events se
                    WHERE
                        se.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        se.repo_name
                ),
                ggazer_score AS (
                    SELECT
                        ri.name_with_owner AS repo_name,
                        ri.primary_language,
                        ROUND(
                            (COALESCE(cn.commiters_count, 0) * 5) +
                            (COALESCE(con.organizations_count, 0) * 10) +
                            (COALESCE(csn.sessions_count, 0) * 3) +
                            (COALESCE(is.opened_issues_count, 0) * 2) +
                            (COALESCE(is.closed_issues_count, 0) * 3) +
                            (COALESCE(fn.forks_count, 0) * 4) +
                            (COALESCE(nbn.new_branches_count, 0) * 1) +
                            (COALESCE(ntn.new_tags_count, 0) * 2) +
                            (COALESCE(ds.pages_updated_count, 0) * 2) +
                            (COALESCE(nmn.new_members_count, 0) * 5) +
                            (COALESCE(sn.stargazers_count, 0) * 1) +
                            (COALESCE(is.resolved_issue_rate, 0) * 20)
                        , 2) AS ggazer_score
                    FROM
                        repos_info ri
                    LEFT JOIN commiters_num cn ON ri.name_with_owner = cn.repo_name
                    LEFT JOIN commiters_organizations_num con ON ri.name_with_owner = con.repo_name
                    LEFT JOIN coding_sessions_num csn ON ri.name_with_owner = csn.repo_name
                    LEFT JOIN issues_stats is ON ri.name_with_owner = is.repo_name
                    LEFT JOIN forks_num fn ON ri.name_with_owner = fn.repo_name
                    LEFT JOIN new_branches_num nbn ON ri.name_with_owner = nbn.repo_name
                    LEFT JOIN new_tags_num ntn ON ri.name_with_owner = ntn.repo_name
                    LEFT JOIN docs_stats ds ON ri.name_with_owner = ds.repo_name
                    LEFT JOIN new_members_num nmn ON ri.name_with_owner = nmn.repo_name
                    LEFT JOIN stargazers_num sn ON ri.name_with_owner = sn.repo_name
                ),
                ggazer_rankings AS (
                    SELECT
                        repo_name,
                        ggazer_score,
                        CASE
                            WHEN ggazer_score >= 200 THEN 'Highly Active'
                            WHEN ggazer_score >= 100 THEN 'Active'
                            WHEN ggazer_score >= 50 THEN 'Moderate'
                            WHEN ggazer_score >= 20 THEN 'Low'
                            ELSE 'Inactive'
                        END AS activity_label,
                        RANK() OVER (ORDER BY ggazer_score DESC) AS ggazer_rank
                    FROM
                        ggazer_score
                )
                SELECT
                    DATE('{period_start_date}') AS period_start_date,
                    DATE('{period_end_date}') AS period_end_date,
                    ri.*,
                    COALESCE(bs.best_streak, 0) AS best_streak,
                    COALESCE(csnm.sessions_count, 0) AS coding_sessions_count,
                    COALESCE(mca.actor_login, 'N/A') AS most_comments_actor,
                    COALESCE(mca.comments_count, 0) AS most_commenter_comments_count,
                    COALESCE(cn.commiters_count, 0) AS commiters_count,
                    COALESCE(con.organizations_count, 0) AS commiters_organizations_count,
                    COALESCE(nbn.new_branches_count, 0) AS new_branches_count,
                    COALESCE(ntn.new_tags_count, 0) AS new_tags_count,
                    COALESCE(is.opened_issues_count, 0) AS opened_issues_count,
                    COALESCE(is.closed_issues_count, 0) AS closed_issues_count,
                    COALESCE(is.resolved_issue_rate, 0.0) AS resolved_issue_rate,
                    COALESCE(ouit.oldest_unresolved_issue_title, 'N/A') AS oldest_unresolved_issue_title,
                    COALESCE(mci.most_commented_issue_title, 'N/A') AS most_commented_issue_title,
                    COALESCE(mci.comments_count, 0) AS most_commented_issue_comments_count,
                    COALESCE(fs.forks_count, 0) AS period_forks_count,
                    COALESCE(ds.pages_updated_count, 0) AS docs_pages_updated_count,
                    ds.docs_updated_flag AS docs_updated_flag,
                    COALESCE(nmn.new_members_count, 0) AS new_members_count,
                    COALESCE(prs.opened_pull_requests_count, 0) AS opened_pull_requests_count,
                    COALESCE(prs.closed_pull_requests_count, 0) AS closed_pull_requests_count,
                    COALESCE(mpra.actor_login, 'N/A') AS most_pull_request_actor,
                    COALESCE(sn.stargazers_count, 0) AS period_stargazers_count,
                    COALESCE(gs.ggazer_score, 0.0) AS ggazer_score,
                    COALESCE(gr.activity_label, 'Inactive') AS activity_label,
                    COALESCE(gr.ggazer_rank, 0) AS ggazer_rank
                FROM
                    repos_info ri
                LEFT JOIN
                    best_streak bs
                ON
                    ri.name_with_owner = bs.repo_name
                LEFT JOIN
                    coding_sessions_num csnm
                ON
                    ri.name_with_owner = csnm.repo_name
                LEFT JOIN
                    most_comments_actor mca
                ON
                    ri.name_with_owner = mca.repo_name
                LEFT JOIN
                    commiters_num cn
                ON
                    ri.name_with_owner = cn.repo_name
                LEFT JOIN
                    commiters_organizations_num con
                ON
                    ri.name_with_owner = con.repo_name
                LEFT JOIN
                    new_branches_num nbn
                ON
                    ri.name_with_owner = nbn.repo_name
                LEFT JOIN
                    new_tags_num ntn
                ON
                    ri.name_with_owner = ntn.repo_name
                LEFT JOIN
                    issues_stats is
                ON
                    ri.name_with_owner = is.repo_name
                LEFT JOIN
                    oldest_unresolved_issue_title ouit
                ON
                    ri.name_with_owner = ouit.repo_name
                LEFT JOIN
                    most_commented_issue mci
                ON
                    ri.name_with_owner = mci.repo_name
                LEFT JOIN
                    forks_num fs
                ON
                    ri.name_with_owner = fs.repo_name
                LEFT JOIN
                    docs_stats ds
                ON
                    ri.name_with_owner = ds.repo_name
                LEFT JOIN
                    new_members_num nmn
                ON
                    ri.name_with_owner = nmn.repo_name
                LEFT JOIN
                    pull_requests_stats prs
                ON
                    ri.name_with_owner = prs.repo_name
                LEFT JOIN
                    most_pull_recuest_actor mpra
                ON
                    ri.name_with_owner = mpra.repo_name
                LEFT JOIN
                    stargazers_num sn
                ON
                    ri.name_with_owner = sn.repo_name
                LEFT JOIN
                    ggazer_score gs
                ON
                    ri.name_with_owner = gs.repo_name
                LEFT JOIN
                    ggazer_rankings gr
                ON
                    ri.name_with_owner = gr.repo_name
                ORDER BY
                    ggazer_rank ASC

            """
        )

        return repo_level_stats

    def calculate_user_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:
        user_level_stats = self.spark_session.sql(
            f"""
                WITH users_info AS (
                    SELECT
                        a.login,
                        a.name,
                        a.avatar_url,
                        a.email,
                        a.website_url,
                        a.description,
                        a.company,
                        a.location
                    FROM
                        ggazers.silver.dim_actor a
                    WHERE
                        type = 'User'
                ),
                branches_num AS (
                    SELECT
                        pe.actor_login,
                        COUNT(DISTINCT pe.ref) AS branches_count
                    FROM
                        ggazers.silver.fact_create_events pe
                    WHERE
                        pe.ref_type = 'branch' AND
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pe.actor_login
                ),
                tags_num AS (
                    SELECT
                        pe.actor_login,
                        COUNT(DISTINCT pe.ref) AS tags_count
                    FROM
                        ggazers.silver.fact_create_events pe
                    WHERE
                        pe.ref_type = 'tag' AND
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pe.actor_login
                ),
                coding_sessions_num AS (
                    SELECT
                        cs.actor_login,
                        COUNT(*) AS sessions_count
                    FROM
                        ggazers.silver.dim_coding_session cs
                    WHERE
                        cs.session_start BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        cs.actor_login
                ),
                most_sessions_repo AS (
                    SELECT
                        actor_login,
                        repo_name,
                        sessions_count_per_repo
                    FROM (
                        SELECT
                            actor_login,
                            exploded_repo AS repo_name,
                            COUNT(*) AS sessions_count_per_repo,
                            ROW_NUMBER() OVER (PARTITION BY actor_login ORDER BY COUNT(*) DESC) AS rn
                        FROM
                            ggazers.silver.dim_coding_session
                        LATERAL VIEW explode(split(repos, ',')) AS exploded_repo
                        WHERE
                            session_start BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        GROUP BY
                            actor_login, exploded_repo
                    )
                    WHERE
                        rn = 1
                ),
                commits_num AS (
                    SELECT
                        pe.actor_login,
                        COUNT(*) AS commit_count
                    FROM
                        ggazers.silver.fact_push_events pe
                    WHERE
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pe.actor_login
                ),
                most_commited_org AS (
                    SELECT
                        actor_login,
                        owner,
                        company,
                        type,
                        commit_count
                    FROM (
                        SELECT
                            pe.actor_login,
                            split(pe.repo_name, '/')[0] AS owner,
                            dim_actor.company,
                            dim_actor.type,
                            COUNT(*) AS commit_count,
                            ROW_NUMBER() OVER (PARTITION BY pe.actor_login ORDER BY COUNT(*) DESC) AS rn
                        FROM
                            ggazers.silver.fact_push_events pe
                        JOIN
                            ggazers.silver.dim_actor AS dim_actor
                        ON
                            split(pe.repo_name, '/')[0] = dim_actor.login
                        WHERE
                            pe.actor_login != split(pe.repo_name, '/')[0] AND
                            dim_actor.type = 'Organization'
                        GROUP BY
                            pe.actor_login, split(pe.repo_name, '/')[0], dim_actor.company, dim_actor.type
                    ) ranked
                    WHERE rn = 1
                ),
                longest_coding_session AS (
                    SELECT
                        cs.actor_login,
                        MAX(
                            UNIX_TIMESTAMP(cs.session_end) - UNIX_TIMESTAMP(cs.session_start)
                        ) AS longest_session_seconds
                    FROM
                        ggazers.silver.dim_coding_session cs
                    WHERE
                        cs.session_start BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        cs.actor_login
                ),
                opened_pull_requests_num AS (
                    SELECT
                        pre.actor_login,
                        COUNT(*) AS opened_pull_requests_count
                    FROM
                        ggazers.silver.fact_pull_request_events pre
                    WHERE
                        pre.action = 'opened' AND
                        pre.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        pre.actor_login
                ),
                ggazer_score AS (
                    SELECT
                        ui.login AS actor_login,
                        ROUND(
                            (COALESCE(bn.branches_count, 0) * 2) +
                            (COALESCE(tn.tags_count, 0) * 2) +
                            (COALESCE(csn.sessions_count, 0) * 3) +
                            (COALESCE(cn.commit_count, 0) * 5) +
                            (COALESCE(oprn.opened_pull_requests_count, 0) * 4)
                        , 2) AS ggazers_score
                    FROM
                        users_info ui
                    LEFT JOIN branches_num bn ON ui.login = bn.actor_login
                    LEFT JOIN tags_num tn ON ui.login = tn.actor_login
                    LEFT JOIN coding_sessions_num csn ON ui.login = csn.actor_login
                    LEFT JOIN commits_num cn ON ui.login = cn.actor_login
                    LEFT JOIN opened_pull_requests_num oprn ON ui.login = oprn.actor_login
                ),
                ggazer_rankings AS (
                    SELECT
                        actor_login,
                        ggazers_score,
                        CASE
                            WHEN ggazers_score >= 100 THEN 'Highly Active'
                            WHEN ggazers_score >= 50 THEN 'Active'
                            WHEN ggazers_score >= 20 THEN 'Moderate'
                            WHEN ggazers_score >= 10 THEN 'Low'
                            ELSE 'Inactive'
                        END AS activity_label,
                        RANK() OVER (ORDER BY ggazers_score DESC) AS ggazer_rank
                    FROM
                        ggazer_score
                )
                SELECT
                    DATE('{period_start_date}') AS period_start_date,
                    DATE('{period_end_date}') AS period_end_date,
                    ui.*,
                    COALESCE(bn.branches_count, 0) AS branches_count,
                    COALESCE(tn.tags_count, 0) AS tags_count,
                    COALESCE(csn.sessions_count, 0) AS coding_sessions_count,
                    COALESCE(msr.repo_name, 'N/A') AS most_sessions_repo,
                    COALESCE(msr.sessions_count_per_repo, 0) AS most_sessions_repo_sessions_count,
                    COALESCE(cn.commit_count, 0) AS commits_count,
                    COALESCE(mco.owner, 'N/A') AS most_commited_organization,
                    lcs.longest_session_seconds AS longest_coding_session_seconds,
                    COALESCE(oprn.opened_pull_requests_count, 0) AS opened_pull_requests_count,
                    COALESCE(gs.ggazers_score, 0.0) AS ggazers_score,
                    COALESCE(gr.activity_label, 'Inactive') AS activity_label,
                    COALESCE(gr.ggazer_rank, 0) AS ggazer_rank
                FROM
                    users_info ui
                LEFT JOIN
                    branches_num bn
                ON
                    ui.login = bn.actor_login
                LEFT JOIN
                    tags_num tn
                ON
                    ui.login = tn.actor_login
                LEFT JOIN
                    coding_sessions_num csn
                ON
                    ui.login = csn.actor_login
                LEFT JOIN
                    most_sessions_repo msr
                ON
                    ui.login = msr.actor_login
                LEFT JOIN
                    commits_num cn
                ON
                    ui.login = cn.actor_login
                LEFT JOIN
                    most_commited_org mco
                ON
                    ui.login = mco.actor_login
                LEFT JOIN
                    longest_coding_session lcs
                ON
                    ui.login = lcs.actor_login
                LEFT JOIN
                    opened_pull_requests_num oprn
                ON
                    ui.login = oprn.actor_login
                LEFT JOIN
                    ggazer_score gs
                ON
                    ui.login = gs.actor_login
                LEFT JOIN
                    ggazer_rankings gr
                ON
                    ui.login = gr.actor_login
                ORDER BY
                    ggazer_rank ASC
            """
        )

        return user_level_stats

    def calculate_org_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:
        org_level_stats = self.spark_session.sql(
            f"""
                WITH org_info AS (
                    SELECT
                        o.login,
                        o.name,
                        o.avatar_url,
                        o.email,
                        o.website_url,
                        o.description,
                        o.company,
                        o.location,
                        o.repositories_count as total_repos_count
                    FROM
                        ggazers.silver.dim_actor o
                    WHERE
                        type = 'Organization'
                ),
                mosts_active_repo AS (
                    SELECT
                        repo_owner,
                        repo_name,
                        activity_count
                    FROM (
                        SELECT
                            repo_owner,
                            repo_name,
                            COUNT(*) AS activity_count,
                            ROW_NUMBER() OVER (PARTITION BY repo_owner ORDER BY COUNT(*) DESC) AS rn
                        FROM (
                            SELECT
                                split(repo_name, '/')[0] AS repo_owner,
                                repo_name
                            FROM
                                ggazers.silver.fact_push_events
                            WHERE
                                created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                            UNION ALL
                            SELECT
                                split(repo_name, '/')[0] AS repo_owner,
                                repo_name
                            FROM
                                ggazers.silver.fact_pull_request_events
                            WHERE
                                created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        )
                        GROUP BY
                            repo_owner, repo_name
                    ) ranked
                    WHERE rn = 1
                ),
                commits_num AS (
                    SELECT
                        split(pe.repo_name, '/')[0] AS repo_owner,
                        COUNT(*) AS commit_count
                    FROM
                        ggazers.silver.fact_push_events pe
                    WHERE
                        pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        split(pe.repo_name, '/')[0]
                ),
                most_active_member AS (
                    SELECT
                        actor_login,
                        COUNT(*) AS activity_count
                    FROM (
                        SELECT
                            pe.actor_login
                        FROM
                            ggazers.silver.fact_push_events pe
                        WHERE
                            pe.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                        UNION ALL
                        SELECT
                            pre.actor_login
                        FROM
                            ggazers.silver.fact_pull_request_events pre
                        WHERE
                            pre.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    )
                    GROUP BY
                        actor_login
                    ORDER BY
                        activity_count DESC
                    LIMIT 1
                ),
                new_stargazers_num AS (
                    SELECT
                        split(se.repo_name, '/')[0] AS repo_owner,
                        COUNT(*) AS new_stargazers_count
                    FROM
                        ggazers.silver.fact_watch_events se
                    WHERE
                        se.created_at BETWEEN DATE('{period_start_date}') AND DATE('{period_end_date}')
                    GROUP BY
                        split(se.repo_name, '/')[0]
                ),
                ggazer_score AS (
                    SELECT
                        oi.login AS org_login,
                        ROUND(
                            (COALESCE(mar.activity_count, 0) * 5) +
                            (COALESCE(cn.commit_count, 0) * 3) +
                            (COALESCE(mam.activity_count, 0) * 4) +
                            (COALESCE(nsn.new_stargazers_count, 0) * 2) +
                            (COALESCE(oi.total_repos_count, 0) * 1)
                        , 2) AS ggazers_score
                    FROM
                        org_info oi
                    LEFT JOIN mosts_active_repo mar ON oi.login = mar.repo_owner
                    LEFT JOIN commits_num cn ON oi.login = cn.repo_owner
                    LEFT JOIN most_active_member mam ON oi.login = mam.actor_login
                    LEFT JOIN new_stargazers_num nsn ON oi.login = nsn.repo_owner
                ),
                ggazer_rankings AS (
                    SELECT
                        org_login,
                        ggazers_score,
                        CASE
                            WHEN ggazers_score >= 150 THEN 'Highly Active'
                            WHEN ggazers_score >= 75 THEN 'Active'
                            WHEN ggazers_score >= 30 THEN 'Moderate'
                            WHEN ggazers_score >= 15 THEN 'Low'
                            ELSE 'Inactive'
                        END AS activity_label,
                        RANK() OVER (ORDER BY ggazers_score DESC) AS ggazer_rank
                    FROM
                        ggazer_score
                )
                SELECT
                    DATE('{period_start_date}') AS period_start_date,
                    DATE('{period_end_date}') AS period_end_date,
                    oi.*,
                    COALESCE(mar.repo_name, 'N/A') AS most_active_repo,
                    COALESCE(mar.activity_count, 0) AS most_active_repo_activity_count,
                    COALESCE(cn.commit_count, 0) AS commits_count,
                    COALESCE(mam.actor_login, 'N/A') AS most_active_member,
                    COALESCE(mam.activity_count, 0) AS most_active_member_activity_count,
                    COALESCE(nsn.new_stargazers_count, 0) AS new_stargazers_count,
                    COALESCE(gs.ggazers_score, 0.0) AS ggazers_score,
                    COALESCE(gr.activity_label, 'Inactive') AS activity_label,
                    COALESCE(gr.ggazer_rank, 0) AS ggazer_rank
                FROM
                    org_info oi
                LEFT JOIN
                    mosts_active_repo mar
                ON
                    oi.login = mar.repo_owner
                LEFT JOIN
                    commits_num cn
                ON
                    oi.login = cn.repo_owner
                LEFT JOIN
                    most_active_member mam
                ON
                    oi.login = mam.actor_login
                LEFT JOIN
                    new_stargazers_num nsn
                ON
                    oi.login = nsn.repo_owner
                LEFT JOIN
                    ggazer_score gs
                ON
                    oi.login = gs.org_login
                LEFT JOIN
                    ggazer_rankings gr
                ON
                    oi.login = gr.org_login
                ORDER BY
                    ggazer_rank ASC
        """
        )
        return org_level_stats
