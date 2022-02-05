from abc import ABC, abstractmethod
from typing import Optional
import datetime


class DataScraper(ABC):

    @abstractmethod
    def scrape(self, timestamp_min: int, before_time:Optional[datetime.datetime], num_queries: Optional[int] = None):
        pass