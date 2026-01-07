import json
import logging

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from constants import (
    BROKER_URL,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    GROUP_ID,
    SCHEMA_REGISTRY_URL,
)
from db_client import DBClient

logger = logging.getLogger(__name__)


class Connector:
    def __init__(
        self,
        schema_registry_url: str = SCHEMA_REGISTRY_URL,
        broker_url: str = BROKER_URL,
        group_id: str = GROUP_ID,
    ):
        self.schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({"url": schema_registry_url})
        self.avro_deserializer: AvroDeserializer = AvroDeserializer(self.schema_registry_client)
        consumer_conf = {
            "bootstrap.servers": broker_url,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": self.avro_deserializer,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        self.consumer = DeserializingConsumer(consumer_conf)
        self.db_client = DBClient(
            db_host=DB_HOST, db_port=DB_PORT, db_name=DB_NAME, db_user=DB_USER, db_password=DB_PASSWORD
        )

    def connect(self):
        self.consumer.subscribe([self.topic])

        print(f"RepoKpiConnector consuming from {self.topic}...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                key = msg.key()
                value = msg.value()
                value[self.map_col] = json.dumps(value[self.map_col])
                print(f"Consumed record with key: {key}, value: {value}")

                if value:
                    update_fields = [k for k in value.keys() if k not in self.conflict_fields]
                    self.db_client.upsert_data(self.table, [value], self.conflict_fields, update_fields)
                self.consumer.commit(message=msg)

        except KeyboardInterrupt:
            logger.warning("Consumer interrupted")
        finally:
            self.consumer.close()
            self.db_client.release_connection()


class RepoKpiConnector(Connector):
    def __init__(
        self,
        topic: str,
        table: str,
        conflict_fields=["name_with_owner", "window_start_utc"],
        map_col="committers_map",
    ):
        super().__init__()
        self.topic = topic
        self.table = table
        self.conflict_fields = conflict_fields
        self.map_col = map_col


class ActorKpiConnector(Connector):
    def __init__(
        self, topic: str, table: str, conflict_fields=["actor_login", "window_start_utc"], map_col="repos_map"
    ):
        super().__init__()
        self.topic = topic
        self.table = table
        self.conflict_fields = conflict_fields
        self.map_col = map_col
