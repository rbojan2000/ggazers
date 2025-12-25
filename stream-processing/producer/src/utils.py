from typing import Any, Dict, List, Tuple


def extract_repo_and_actor_names(events: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    repos = {event["repo"]["name"] for event in events}
    actors = {event["actor"]["login"] for event in events}
    actors.update(repo.split("/")[0] for repo in repos)

    return list(repos), list(actors)
