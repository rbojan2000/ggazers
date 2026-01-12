from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {"owner": "Bojan Radovic", "retries": 3, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="github_etl",
    default_args=default_args,
    start_date=datetime(2024, 3, 1, 2),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    ingestion = SSHOperator(
        task_id="ingestion",
        ssh_conn_id="1",
        command="cd /opt/app/ingestion/ && " "poetry run python3 -m src.runner",
    )

    transform = SSHOperator(
        task_id="transform",
        ssh_conn_id="1",
        command="cd /opt/app/transformation/ && " "python3 -m src.runner --dataset actors",
    )

    load_repo_level_stats = SSHOperator(
        task_id="load_repo_level_stats",
        ssh_conn_id="1",
        command="cd /opt/app/load/ && " "python3 -m src.runner --dataset repo_level_stats",
    )

    load_org_level_stats = SSHOperator(
        task_id="load_org_level_stats",
        ssh_conn_id="1",
        command="cd /opt/app/load/ && " "python3 -m src.runner --dataset org_level_stats",
    )
    load_user_level_stats = SSHOperator(
        task_id="load_user_level_stats",
        ssh_conn_id="1",
        command="cd /opt/app/load/ && " "python3 -m src.runner --dataset user_level_stats",
    )

    (ingestion >> transform >> load_org_level_stats >> load_user_level_stats >> load_repo_level_stats)
