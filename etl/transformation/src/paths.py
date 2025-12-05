from pathlib import Path

ROOT_PATH = Path(__file__).absolute().parent.parent

DATA_PATH = ROOT_PATH.parent.parent / "data"

BRONZE_DATA_PATH = DATA_PATH / "bronze"

GITHUB_EVENTS_PATH = BRONZE_DATA_PATH / "github_events"

ACTORS_PATH = BRONZE_DATA_PATH / "actors"

REPOS_PATH = BRONZE_DATA_PATH / "repos"

GITHUB_EVENTS_PATH = BRONZE_DATA_PATH / "github_events"

SILVER_PATH = DATA_PATH / "silver"
