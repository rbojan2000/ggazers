from dataclasses import dataclass
from typing import Optional

from dataclasses_avroschema import AvroModel


@dataclass
class Actor(AvroModel):
    login: str
    name: Optional[str]
    email: Optional[str]
    bio: Optional[str]
    company: Optional[str]
    location: Optional[str]
    websiteUrl: Optional[str]
    type: Optional[str] = None


@dataclass
class Repo(AvroModel):
    name_with_owner: Optional[str]
    description: Optional[str]
    created_at: Optional[str]
    disk_usage: Optional[int]
    visibility: Optional[str]
    stargazers_count: Optional[int]
    forks_count: Optional[int]
    watchers_count: Optional[int]
    issues_count: Optional[int]
    primary_language: Optional[str]


@dataclass
class GitHubEvent(AvroModel):
    id: str
    actor_login: str
    repo_name: str
    event_type: str
    created_at: str


@dataclass
class PushEvent(AvroModel):
    event: GitHubEvent
    ref: Optional[str]


@dataclass
class ReleaseEvent(AvroModel):
    event: GitHubEvent
    action: Optional[str]
    release_tag: Optional[str]


@dataclass
class PullRequestEvent(AvroModel):
    event: GitHubEvent
    action: Optional[str]
    pr_number: Optional[int]


@dataclass
class MessageKey(AvroModel):
    key: str
