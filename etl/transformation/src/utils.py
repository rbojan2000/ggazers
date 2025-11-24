import calendar
from datetime import date, datetime
from typing import Tuple


def get_first_and_last_day_of_month(year: int, month: int) -> Tuple[date, date]:
    first_day = date(year, month, 1)
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    return first_day, last_day


def generate_file_name(date: datetime, part: int) -> str:
    return f"{date.strftime('%Y_%m_%d')}_{part}.json"
