from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

from src.schemas import Actor, GitHubEvent, PullRequestEvent, PushEvent, ReleaseEvent, Repo


class Mapper(ABC):
    @abstractmethod
    def map_messages(self, messages: List[Dict[str, Any]]) -> List[Any]:
        pass


class ActorMapper(Mapper):
    def map_messages(self, messages: List[Dict[str, Any]]) -> List[Actor]:
        actors: List[Actor] = []
        for message in messages:
            actor: Actor = Actor(
                type=message.get("__typename"),
                login=message.get("login"),
                name=message.get("name"),
                email=message.get("email"),
                bio=message.get("bio"),
                company=message.get("company"),
                location=message.get("location"),
                websiteUrl=message.get("websiteUrl"),
            )
            actors.append(actor)
        return actors


class RepoMapper(Mapper):
    def map_messages(self, messages: List[Dict[str, Any]]) -> Any:
        repos: List[Repo] = []
        for message in messages:
            repo: Repo = Repo(
                name_with_owner=message.get("nameWithOwner"),
                owner=message.get("nameWithOwner").split("/")[0],
                name=message.get("nameWithOwner").split("/")[1],
                description=message.get("description"),
                created_at=message.get("createdAt"),
                disk_usage=message.get("diskUsage"),
                visibility=message.get("visibility"),
                stargazers_count=message.get("stargazerCount"),
                forks_count=message.get("forkCount"),
                watchers_count=message.get("watchers").get("totalCount"),
                issues_count=message.get("issues").get("totalCount"),
                primary_language=(
                    message.get("primaryLanguage").get("name") if message.get("primaryLanguage") else None
                ),
            )
            repos.append(repo)
        return repos


class GitHubEventMapper(Mapper):
    def map_messages(
        self, messages: List[Dict[str, Any]]
    ) -> Tuple[List[PushEvent], List[ReleaseEvent], List[PullRequestEvent]]:
        push_events: List[PushEvent] = []
        release_events: List[ReleaseEvent] = []
        pr_events: List[PullRequestEvent] = []

        for msg in messages:
            match msg.get("type"):
                case "PushEvent":
                    push_events.append(self._map_push_event(msg))
                case "ReleaseEvent":
                    release_events.append(self._map_release_event(msg))
                case "PullRequestEvent":
                    pr_events.append(self._map_pr_event(msg))
        return push_events, release_events, pr_events

    def _map_event(self, msg: Dict[str, Any]) -> GitHubEvent:
        return GitHubEvent(
            id=msg["id"],
            actor_login=msg["actor"]["login"],
            repo_name=msg["repo"]["name"],
            event_type=msg["type"],
            created_at=msg["created_at"],
        )

    def _map_push_event(self, msg: Dict[str, Any]) -> PushEvent:
        github_event = self._map_event(msg)
        push_event = PushEvent(
            event=github_event,
            ref=msg["payload"].get("ref"),
        )
        return push_event

    def _map_release_event(self, msg: Dict[str, Any]) -> ReleaseEvent:
        github_event = self._map_event(msg)
        release_event = ReleaseEvent(
            event=github_event,
            action=msg["payload"].get("action"),
            release_tag=msg["payload"].get("release", {}).get("tag_name"),
        )
        return release_event

    def _map_pr_event(self, msg: Dict[str, Any]) -> PullRequestEvent:
        github_event = self._map_event(msg)
        pr_event = PullRequestEvent(
            event=github_event,
            action=msg["payload"].get("action"),
            pr_number=msg["payload"].get("number"),
        )
        return pr_event
