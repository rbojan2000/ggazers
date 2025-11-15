from pathlib import Path

ROOT_PATH = Path(__file__).absolute().parent.parent

DATA_PATH = ROOT_PATH.parent.parent / "data"

BRONZE_DATA_PATH = DATA_PATH / "bronze"
