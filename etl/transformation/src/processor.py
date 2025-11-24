from datetime import date

from pyspark.sql import DataFrame, SparkSession


class DataProcessor:

    @classmethod
    def transform_actors(cls, spark: SparkSession, start_date: date, end_date: date) -> DataFrame:
        pass
