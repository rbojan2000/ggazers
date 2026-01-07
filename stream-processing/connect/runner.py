import logging

import click
from connector.connector import ActorKpiConnector, Connector, RepoKpiConnector
from constants import REPO_KPI_TABLE, REPO_KPI_TOPIC, USER_KPI_TABLE, USER_KPI_TOPIC

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--dataset",
    required=True,
    type=click.Choice(["repo_kpi", "actor_kpi"], case_sensitive=False),
    help="Specify the dataset to transform",
)
def run(dataset: str) -> None:
    logger.info(f"Starting connector for dataset: {dataset}")
    connector: Connector = None

    match dataset.lower():
        case "repo_kpi":
            connector = RepoKpiConnector(topic=REPO_KPI_TOPIC, table=REPO_KPI_TABLE)
        case "actor_kpi":
            connector = ActorKpiConnector(topic=USER_KPI_TOPIC, table=USER_KPI_TABLE)

    connector.connect()


if __name__ == "__main__":
    run()
