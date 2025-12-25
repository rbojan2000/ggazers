from abc import ABC, abstractmethod
from typing import Any, List

from confluent_kafka.avro import AvroProducer
from constants import (
    ACTORS_TOPIC,
    PR_EVENTS_TOPIC,
    PUSH_EVENTS_TOPIC,
    RELEASE_EVENTS_TOPIC,
    REPOS_TOPIC,
)
from src.schemas import Actor, MessageKey, PullRequestEvent, PushEvent, ReleaseEvent, Repo


class KafkaProducer(ABC):
    def __init__(
        self,
        broker_url: str,
        schema_registry_url: str,
        avro_producer: AvroProducer = None,
        broker_address_family: str = "v4",
    ):
        self.broker_url = broker_url
        self.schema_registry_url = schema_registry_url
        self.avro_producer = avro_producer
        self.broker_address_family = broker_address_family
        self.init_avro_producer()

    @abstractmethod
    def publish_messages(self, messages: List[Any]) -> None:
        pass

    @abstractmethod
    def _build_key(self, message: Any) -> MessageKey:
        pass

    def init_avro_producer(self) -> AvroProducer:
        avro_producer = AvroProducer(
            config={
                "bootstrap.servers": self.broker_url,
                "schema.registry.url": self.schema_registry_url,
                "broker.address.family": self.broker_address_family,
            },
        )
        self.avro_producer = avro_producer
        return avro_producer


class GithubEventProducer(KafkaProducer):
    def __init__(self, broker_url, schema_registry_url, avro_producer=None):
        super().__init__(broker_url, schema_registry_url, avro_producer)

    def _build_key(self, event) -> MessageKey:
        return MessageKey(key=event.event.id)

    def _publish_push_event(self, event: PushEvent) -> None:
        key = self._build_key(event)
        self.avro_producer.produce(
            topic=PUSH_EVENTS_TOPIC,
            value=event.to_dict(),
            value_schema=PushEvent.avro_schema(),
            key=key.to_dict(),
            key_schema=MessageKey.avro_schema(),
        )

    def _publish_release_event(self, event: ReleaseEvent) -> None:
        key = self._build_key(event.event)
        self.avro_producer.produce(
            topic=RELEASE_EVENTS_TOPIC,
            value=event.to_dict(),
            value_schema=ReleaseEvent.avro_schema(),
            key=key.to_dict(),
            key_schema=MessageKey.avro_schema(),
        )

    def _publish_pr_event(self, event: PullRequestEvent) -> None:
        key = self._build_key(event.event)
        self.avro_producer.produce(
            topic=PR_EVENTS_TOPIC,
            value=event.to_dict(),
            value_schema=PullRequestEvent.avro_schema(),
            key=key.to_dict(),
            key_schema=MessageKey.avro_schema(),
        )

    def publish_messages(
        self,
        push_events: List[PushEvent],
        release_events: List[ReleaseEvent],
        pr_events: List[PullRequestEvent],
    ) -> None:
        for event in push_events:
            self._publish_push_event(event)

        for event in release_events:
            self._publish_release_event(event)

        for event in pr_events:
            self._publish_pr_event(event)

        self.avro_producer.flush()


class ActorProducer(KafkaProducer):
    def __init__(self, broker_url, schema_registry_url, avro_producer=None):
        super().__init__(broker_url, schema_registry_url, avro_producer)

    def _build_key(self, actor: Actor) -> MessageKey:
        return MessageKey(key=actor.login)

    def publish_messages(self, actors: List[Actor]) -> None:
        for actor in actors:
            key = self._build_key(actor)
            self.avro_producer.produce(
                topic=ACTORS_TOPIC,
                key=key.to_dict(),
                key_schema=MessageKey.avro_schema(),
                value=actor.to_dict(),
                value_schema=Actor.avro_schema(),
            )

        self.avro_producer.flush()


class RepoProducer(KafkaProducer):
    def __init__(self, broker_url, schema_registry_url, avro_producer=None):
        super().__init__(broker_url, schema_registry_url, avro_producer)

    def _build_key(self, repo: Repo) -> MessageKey:
        return MessageKey(key=repo.name_with_owner)

    def publish_messages(self, repos: List[Repo]) -> None:
        for repo in repos:
            key = self._build_key(repo)
            self.avro_producer.produce(
                topic=REPOS_TOPIC,
                value=repo.to_dict(),
                value_schema=Repo.avro_schema(),
                key=key.to_dict(),
                key_schema=MessageKey.avro_schema(),
            )

        self.avro_producer.flush()
