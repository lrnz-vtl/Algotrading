import datetime
from abc import ABC, abstractmethod


class PriceSignalProvider(ABC):

    @abstractmethod
    def update(self, t: datetime.datetime, price: float) -> None:
        pass

    @abstractmethod
    def value(self, t: datetime.datetime) -> float:
        pass


class DummySignalProvider(PriceSignalProvider):

    def update(self, t: datetime.datetime, price: float) -> None:
        return

    def value(self, t: datetime.datetime) -> float:
        return 0.0
