import logging
import os
from typing import Dict

from paths import DATA_PATH
from pyspark.sql import DataFrame, SparkSession
from src.analyzer import Analyzer

logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self) -> None:
        self.spark_session = None
        self.analyzer = None

    def init_spark_session(self) -> None:
        self.spark_session: SparkSession = (
            SparkSession.builder.appName("transformation")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0," "org.postgresql:postgresql:42.6.0",
            )
            .config(
                "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ggazers.type", "hadoop")
            .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate()
        )
        self.analyzer = Analyzer(self.spark_session)

    def terminate_spark_session(self) -> None:
        self.spark_session.stop()

    def calculate_repo_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:
        repo_level_stats = self.analyzer.calculate_repo_level_stats(period_start_date, period_end_date)
        self._write_to_postgres(repo_level_stats, table_name="repo_level_stats")

        logger.info("Repo level stats written to DB.")
        return repo_level_stats

    def _write_to_postgres(
        self,
        df: DataFrame,
        table_name: str,
        jdbc_url: str = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/ggazers"),
        properties: Dict[str, str] = {
            "user": os.getenv("GGAZERS_USER", "ggazers"),
            "password": os.getenv("GGAZERS_PASSWORD", "ggazers123"),
            "driver": "org.postgresql.Driver",
        },
        mode: str = "overwrite",
    ) -> None:
        """
        Write DataFrame to PostgreSQL table.

        Args:
            df: DataFrame to write
            table_name: Target table name
            jdbc_url: PostgreSQL JDBC URL
            properties: Dict with 'user', 'password', 'driver' keys
            mode: Write mode ('overwrite', 'append', 'ignore', 'error')
        """

        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)
