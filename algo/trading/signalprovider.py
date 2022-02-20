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

    def __init__(self, const: float = 0.0, alternate: bool = False):
        self.const = const
        self.alternate = alternate

    def update(self, t: datetime.datetime, price: float) -> None:
        if self.alternate:
            self.const = -self.const

    def value(self, t: datetime.datetime) -> float:
        return self.const
