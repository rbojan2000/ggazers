import calendar
import gzip
from datetime import date, datetime
from typing import Tuple


def get_first_and_last_day_of_month(year: int, month: int) -> Tuple[date, date]:
    first_day = date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def decompress_gzip_data(compressed_data: bytes) -> bytes:
    return gzip.decompress(compressed_data)


def generate_file_name(date: datetime) -> str:
    return f"github_events{date.strftime('%Y_%m_%d')}.jsonl"


def save_data_to_file(data: bytes, filepath: str) -> None:
    with open(filepath, "wb") as f:
        f.write(data)
