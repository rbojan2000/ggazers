import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from src.transformer import Transformer


class TransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("TransformerTest").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_flatten_repository_topics(self):
        data = [
            {
                "name_with_owner": "octocat/hello-world",
                "repositoryTopics": {
                    "nodes": [{"topic": {"name": "etl"}}, {"topic": {"name": "spark"}}]
                },
            }
        ]
        schema = StructType(
            [
                StructField("name_with_owner", StringType(), True),
                StructField(
                    "repositoryTopics",
                    StructType(
                        [
                            StructField(
                                "nodes",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                "topic",
                                                StructType(
                                                    [StructField("name", StringType(), True)]
                                                ),
                                                True,
                                            )
                                        ]
                                    )
                                ),
                                True,
                            )
                        ]
                    ),
                    True,
                ),
            ]
        )

        df = self.spark.createDataFrame(data, schema)

        df = Transformer.flatten_repository_topics(df)
        expected_data = [("octocat/hello-world", "spark,etl")]
        expected_schema = StructType(
            [
                StructField("name_with_owner", StringType(), True),
                StructField("repository_topics", StringType(), True),
            ]
        )
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        self.assertEqual(df.collect(), expected_df.collect())

    def test_no_topics(self):
        data = [{"name_with_owner": "user/repo3", "repositoryTopics": {"nodes": []}}]
        schema = StructType(
            [
                StructField("name_with_owner", StringType(), True),
                StructField(
                    "repositoryTopics",
                    StructType(
                        [
                            StructField(
                                "nodes",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                "topic",
                                                StructType(
                                                    [StructField("name", StringType(), True)]
                                                ),
                                                True,
                                            )
                                        ]
                                    )
                                ),
                                True,
                            )
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(data, schema)
        df_transformed = Transformer.flatten_repository_topics(df)
        result = df_transformed.select("repository_topics").collect()[0].repository_topics
        self.assertEqual(result, None)
