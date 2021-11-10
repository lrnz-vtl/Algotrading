from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from tinyman.v1.pools import SwapQuote


@dataclass
class TradeInfo:
    quote: SwapQuote
    asset1_id: int
    asset2_id: int
    quantity: float
    target_price: float
    slippage: float
    excess_min: float


@dataclass
class TradeLog:
    tradeInfo: TradeInfo
    now: datetime
    utcnow: datetime


class TradeLogger(ABC):
    @abstractmethod
    def log(self, trade: TradeLog):
        pass
