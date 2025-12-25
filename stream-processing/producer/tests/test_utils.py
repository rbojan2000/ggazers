from src.utils import extract_repo_and_actor_names


def test_extract_repo_and_actor_names():
    events = [
        {"repo": {"name": "octocat/Hello-World"}, "actor": {"login": "octocat"}},
        {"repo": {"name": "octocat/Hello-World"}, "actor": {"login": "octocat"}},
        {"repo": {"name": "otheruser/OtherRepo"}, "actor": {"login": "otheruser"}},
    ]
    repos, actors = extract_repo_and_actor_names(events)
    assert set(repos) == {"octocat/Hello-World", "otheruser/OtherRepo"}
    assert "octocat" in actors
    assert "otheruser" in actors
