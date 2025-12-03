from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_set, concat_ws, explode


class Transformer:

    @classmethod
    def flatten_repository_topics(cls, df: DataFrame) -> DataFrame:
        # Use the original nested column name
        df_topics = df.withColumn("topic_node", explode(col("repositoryTopics.nodes"))).withColumn(
            "topic_name", col("topic_node.topic.name")
        )

        df_topics_agg = df_topics.groupBy("name_with_owner").agg(
            concat_ws(",", collect_set("topic_name")).alias("repository_topics")
        )

        df_final = df.drop("repositoryTopics").join(df_topics_agg, on="name_with_owner", how="left")
        return df_final
