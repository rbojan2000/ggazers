import logging
import time

from constants import SLEEP_AFTER_POLL
from producer.kafka_producer import ActorProducer, GithubEventProducer, RepoProducer
from src.github_client import GithubClient
from src.mapper.mapper import ActorMapper, GitHubEventMapper, RepoMapper
from utils import extract_repo_and_actor_names

logger = logging.getLogger(__name__)


class Processor:
    def __init__(
        self,
        github_client: GithubClient,
        event_producer: GithubEventProducer,
        actor_producer: ActorProducer,
        repo_producer: RepoProducer,
        actor_mapper: ActorMapper,
        event_mapper: GitHubEventMapper,
        repo_mapper: RepoMapper,
        sleep_after_poll: int = SLEEP_AFTER_POLL,
    ):
        self.github_client = github_client
        self.event_producer = event_producer
        self.actor_producer = actor_producer
        self.repo_producer = repo_producer
        self.event_mapper = event_mapper
        self.actor_mapper = actor_mapper
        self.repo_mapper = repo_mapper
        self.sleep_after_poll = sleep_after_poll

    def run(self):
        while True:
            logger.info("Fetching events from GitHub API...")
            events = self.github_client.get_events()
            if not events:
                logger.info("No new events or failed to fetch events. Sleeping...")
                time.sleep(self.sleep_after_poll)
                continue

            logger.info(f"Fetched {len(events)} events from GitHub API")

            repo_names, actor_names = extract_repo_and_actor_names(events)

            repos_query = self.github_client.build_graphql_query(repos=repo_names)
            repos_data = self.github_client.run_query(repos_query)
            if not repos_data:
                logger.info("GraphQL query failed, trying REST API...")
                repos_data = self.github_client.hit_rest_api("repos", repos_query)

            actors_query = self.github_client.build_graphql_query(actors=actor_names)
            actors_data = self.github_client.run_query(actors_query)
            if not actors_data:
                logger.info("GraphQL query failed, trying REST API...")
                actors_data = self.github_client.hit_rest_api("actors", actors_query)

            actors = self.actor_mapper.map_messages(actors_data)
            repos = self.repo_mapper.map_messages(repos_data)
            push_events, release_events, pr_events = self.event_mapper.map_messages(events)

            logger.info(
                f"Publishing "
                f"  {len(repos)} repos,"
                f"  {len(actors)} actors,"
                f"  {len(push_events)} push events,"
                f"  {len(release_events)} release events and"
                f"  {len(pr_events)} PR events to Kafka..."
            )
            self.repo_producer.publish_messages(repos)
            self.actor_producer.publish_messages(actors)
            self.event_producer.publish_messages(push_events)

            time.sleep(self.sleep_after_poll)
