from dataclasses import dataclass
from typing import Optional

from dataclasses_avroschema import AvroModel


@dataclass
class GGazersAvroModel(AvroModel):
    class Meta:
        namespace = "ggazers.avro.message"


@dataclass
class Actor(GGazersAvroModel):
    login: str
    name: Optional[str]
    email: Optional[str]
    bio: Optional[str]
    company: Optional[str]
    location: Optional[str]
    websiteUrl: Optional[str]
    type: Optional[str] = None


@dataclass
class Repo(GGazersAvroModel):
    name_with_owner: Optional[str]
    owner: Optional[str]
    name: Optional[str]
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
class GitHubEvent(GGazersAvroModel):
    id: str
    actor_login: str
    repo_name: str
    event_type: str
    created_at: str


@dataclass
class PushEvent(GGazersAvroModel):
    event: GitHubEvent
    ref: Optional[str]


@dataclass
class ReleaseEvent(GGazersAvroModel):
    event: GitHubEvent
    action: Optional[str]
    release_tag: Optional[str]


@dataclass
class PullRequestEvent(GGazersAvroModel):
    event: GitHubEvent
    action: Optional[str]
    pr_number: Optional[int]


@dataclass
class MessageKey(GGazersAvroModel):
    key: str
