import logging
from abc import ABC, abstractmethod
from typing import Any, List

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from constants import ACTORS_TOPIC, BROKER_URL, PUSH_EVENTS_TOPIC, REPOS_TOPIC, SCHEMA_REGISTRY_URL
from src.schemas import Actor, PushEvent, Repo

logger = logging.getLogger(__name__)


class KafkaProducer(ABC):
    def __init__(
        self,
        broker_url: str,
        schema_registry_url: str = SCHEMA_REGISTRY_URL,
    ):
        self.broker_url = broker_url
        self.schema_registry_client: SchemaRegistryClient = SchemaRegistryClient({"url": schema_registry_url})

    @abstractmethod
    def publish_messages(self, messages: List[Any]) -> None:
        pass


class GithubEventProducer(KafkaProducer):
    def __init__(self, broker_url=BROKER_URL, schema_registry_url=SCHEMA_REGISTRY_URL):
        super().__init__(broker_url, schema_registry_url=schema_registry_url)
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            PushEvent.avro_schema(),
        )
        self.producer_conf = {
            "bootstrap.servers": self.broker_url,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
        }
        self.producer = SerializingProducer(self.producer_conf)

    def publish_messages(
        self,
        push_events: List[PushEvent],
    ) -> None:
        for event in push_events:
            logger.info(f"Producing PushEvent {event}")
            self.producer.produce(
                topic=PUSH_EVENTS_TOPIC,
                value=event.to_dict(),
                key=event.event.id,
            )

        self.producer.flush()


class ActorProducer(KafkaProducer):
    def __init__(self, broker_url=BROKER_URL, schema_registry_url=SCHEMA_REGISTRY_URL):
        super().__init__(broker_url, schema_registry_url)
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            Actor.avro_schema(),
        )
        self.producer_conf = {
            "bootstrap.servers": self.broker_url,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
        }
        self.producer = SerializingProducer(self.producer_conf)

    def publish_messages(self, actors: List[Actor]) -> None:
        for actor in actors:
            logger.info(f"Producing Actor {actor}")
            self.producer.produce(
                topic=ACTORS_TOPIC,
                value=actor.to_dict(),
                key=actor.login,
            )

        self.producer.flush()


class RepoProducer(KafkaProducer):
    def __init__(self, broker_url=BROKER_URL, schema_registry_url=SCHEMA_REGISTRY_URL):
        super().__init__(broker_url, schema_registry_url)
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            Repo.avro_schema(),
        )
        self.producer_conf = {
            "bootstrap.servers": self.broker_url,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self.avro_serializer,
        }
        self.producer = SerializingProducer(self.producer_conf)

    def publish_messages(self, repos: List[Repo]) -> None:
        for repo in repos:
            logger.info(f"Producing Repo {repo}")
            self.producer.produce(
                topic=REPOS_TOPIC,
                value=repo.to_dict(),
                key=repo.name_with_owner,
            )

        self.producer.flush()
