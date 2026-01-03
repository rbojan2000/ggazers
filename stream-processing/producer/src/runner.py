from mapper.mapper import ActorMapper, GitHubEventMapper, RepoMapper
from producer.kafka_producer import ActorProducer, GithubEventProducer, RepoProducer
from src.github_client import GithubClient
from src.processor import Processor

github_client = GithubClient()
events_producer = GithubEventProducer()
actors_producer = ActorProducer()
repos_producer = RepoProducer()

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
