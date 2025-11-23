import http
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
    ) -> Generator[Tuple[bytes, date, int], None, None]:

        logger.info(f"Fetching data from {start_date} to {end_date}")
        current_date = start_date

        while current_date <= end_date:
            logger.info(f"Fetching data for date: {current_date}")
            for daily_data, part in self._fetch_for_date(current_date):
                yield daily_data, current_date, part

            current_date += timedelta(days=1)

    def _fetch_for_date(
        self, date: date, parts_per_date: int = 24
    ) -> Generator[Tuple[bytes, int], None, None]:
        for part in range(parts_per_date):
            logger.info(f"Fetching part #{part} for date: {date}")
            chunk = self._fetch_data(date, part)

            if not chunk:
                continue

            yield chunk, part

    def _fetch_data(self, date: date, part: int) -> bytes:
        url = f"{self.api_url}{self._format_date(date)}-{part}.json.gz"
        try:
            response = requests.get(url, timeout=60 * 10)

            if response.status_code != http.HTTPStatus.OK:
                logger.warning(f"Skipping part {part} on {date}: HTTP {response.status_code}")
                return b""

            return response.content

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching part {part} on {date}: {e}")
            return b""

    def _format_date(self, date: date) -> str:
        return date.strftime("%Y-%m-%d")
