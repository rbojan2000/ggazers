import logging

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
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
            .config(
                "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )
            .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ggazers.type", "hadoop")
            .config("spark.sql.catalog.ggazers.warehouse", DATA_PATH)
            .getOrCreate()
        )
        self.analyzer = Analyzer(self.spark_session)

    def terminate_spark_session(self) -> None:
        self.spark_session.stop()

    def calculate_repo_level_stats(self, period_start_date: str, period_end_date: str) -> DataFrame:
        df = self.analyzer.calculate_repo_level_stats(period_start_date, period_end_date)
        df.show()
