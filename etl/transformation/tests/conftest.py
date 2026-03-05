import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any, Generator

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:

    temp_dir = tempfile.mkdtemp()
    warehouse_path = Path(temp_dir) / "warehouse"
    warehouse_path.mkdir()
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("TestSession")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", str(warehouse_path))
        .config("spark.sql.catalog.ggazers", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ggazers.type", "hadoop")
        .config("spark.sql.catalog.ggazers.warehouse", str(warehouse_path))
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
    shutil.rmtree(temp_dir)


@pytest.fixture
def silver_tables(spark_session: SparkSession) -> Generator[None, Any, None]:
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS ggazers.silver")
    spark_session.sql(
        """
        CREATE TABLE IF NOT EXISTS ggazers.silver.dim_actor (
            type STRING,
            login STRING NOT NULL,
            avatar_url STRING,
            name STRING,
            email STRING,
            website_url STRING,
            description STRING,
            company STRING,
            location STRING,
            created_at TIMESTAMP,
            followers_count BIGINT,
            following_count BIGINT,
            repositories_count BIGINT,
            gists_count BIGINT,
            twitter_username STRING,
            status_message STRING,
            updated_at TIMESTAMP
        ) USING ICEBERG
        """
    )
    spark_session.sql(
        """
        CREATE TABLE IF NOT EXISTS ggazers.silver.dim_repo (
            name_with_owner STRING NOT NULL,
            name STRING,
            owner STRING,
            description STRING,
            is_private BOOLEAN,
            is_archived BOOLEAN,
            is_fork BOOLEAN,
            disk_usage BIGINT,
            visibility STRING,
            stargazers_count BIGINT,
            forks_count BIGINT,
            watchers_count BIGINT,
            issues_count BIGINT,
            primary_language STRING,
            repository_topics STRING,
            updated_at TIMESTAMP
        ) USING ICEBERG
        """
    )
    spark_session.sql(
        """
        CREATE TABLE IF NOT EXISTS ggazers.silver.dim_coding_session (
            actor_login STRING NOT NULL,
            session_start TIMESTAMP NOT NULL,
            session_end TIMESTAMP,
            duration BIGINT,
            event_count BIGINT
        ) USING ICEBERG
        """
    )

    yield

    spark_session.sql("DROP TABLE IF EXISTS ggazers.silver.dim_actor")
    spark_session.sql("DROP TABLE IF EXISTS ggazers.silver.dim_repo")
    spark_session.sql("DROP TABLE IF EXISTS ggazers.silver.dim_coding_session")
    spark_session.sql("DROP NAMESPACE IF EXISTS ggazers.silver")
