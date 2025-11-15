import logging
from datetime import date, timedelta
from typing import Generator, Tuple

import requests

logger = logging.getLogger(__name__)


class GHArchiveClient:
    def __init__(self, api_url: str = "https://data.gharchive.org/") -> None:
        self.api_url = api_url

    def get_events_dump(
        self, start_date: date, end_date: date
    ) -> Generator[Tuple[bytes, date], None, None]:

        logger.info(f"Fetching data from #{start_date} to #{end_date}")
        current_date = start_date

        while current_date <= end_date:
            logger.info(f"Fetching data for date: #{current_date}")
            daily_data = self._fetch_for_date(current_date)
            yield daily_data, current_date
            current_date += timedelta(days=1)

    def _fetch_for_date(self, date: date, parts_per_date: int = 2) -> bytes:
        all_data: bytes = b""
        for i in range(parts_per_date):
            logger.info(f"Fetching part #{i} for date: #{date}")
            all_data += self._fetch_data(date, i)

        return all_data

    def _fetch_data(self, date: date, part: int) -> bytes:
        url = f"{self.api_url}{self._format_date(date)}-{part}.json.gz"
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def _format_date(self, date: date) -> str:
        return date.strftime("%Y-%m-%d")

