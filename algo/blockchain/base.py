from abc import ABC, abstractmethod
from typing import Optional


class DataScraper(ABC):

    @abstractmethod
    def scrape(self, timestamp_min: int, num_queries: Optional[int] = None):
        pass