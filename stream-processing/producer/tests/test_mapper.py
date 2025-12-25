from src.mapper.mapper import ActorMapper


def test_actor_mapper_map_messages():
    messages = [
        {
            "__typename": "User",
            "login": "octocat",
            "name": "The Octocat",
            "email": "octocat@github.com",
            "bio": "Test user",
            "company": "GitHub",
            "location": "Internet",
            "websiteUrl": "https://github.com/octocat",
        }
    ]
    mapper = ActorMapper()
    actors = mapper.map_messages(messages)
    assert actors[0].login == "octocat"
    assert actors[0].name == "The Octocat"
