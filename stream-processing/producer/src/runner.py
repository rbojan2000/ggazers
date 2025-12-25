import os

from mapper.mapper import ActorMapper, GitHubEventMapper, RepoMapper
from producer.kafka_producer import ActorProducer, GithubEventProducer, RepoProducer
from src.github_client import GithubClient
from src.processor import Processor

github_client = GithubClient()
events_producer = GithubEventProducer(
    broker_url=os.getenv("BROKER_URL", "127.0.0.1:9092"),
    schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"),
)
actors_producer = ActorProducer(
    broker_url=os.getenv("BROKER_URL", "127.0.0.1:9092"),
    schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"),
)
repos_producer = RepoProducer(
    broker_url=os.getenv("BROKER_URL", "127.0.0.1:9092"),
    schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"),
)

event_mapper = GitHubEventMapper()
actor_mapper = ActorMapper()
repo_mapper = RepoMapper()

processor = Processor(
    github_client=github_client,
    event_mapper=event_mapper,
    actor_mapper=actor_mapper,
    repo_mapper=repo_mapper,
    event_producer=events_producer,
    actor_producer=actors_producer,
    repo_producer=repos_producer,
)


def run():
    processor.run()


if __name__ == "__main__":
    run()
