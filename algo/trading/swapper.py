from tinyman.v1.client import TinymanClient
from algo.tools.timestamp import Timestamp
from algo.trade_logger.base import TradeLogger
from logging import Logger
import logging
from typing import Optional
from tinyman.assets import Asset, AssetAmount, Decimal
from abc import ABC, abstractmethod
from dataclasses import dataclass
from tinyman.v1.pools import SwapQuote
from typing import Callable


@dataclass
class AlgoPoolSwap:
    asset_buy: int
    amount_buy: int
    amount_sell: int


class Swapper(ABC):
    @abstractmethod
    def attempt_transaction(self, quote: SwapQuote) -> Optional[AlgoPoolSwap]:
        pass


# Fake swapper for the simulation environment
class SimulationSwapper(Swapper):

    # def __init__(self, asset1: int, asset2: int, price_provider: Callable[[], tuple[int, int]]):
    #     self.price_provider = price_provider
    #     self.asset1 = asset1
    #     self.asset2 = asset2

    def attempt_transaction(self, quote: SwapQuote) -> Optional[AlgoPoolSwap]:
        return AlgoPoolSwap(
            asset_buy=quote.amount_out.asset.id,
            amount_buy=quote.amount_out.amount,
            amount_sell=quote.amount_in.amount
        )


# What we have in real trading environment
class ProductionSwapper(Swapper):

    def __init__(self, address, private_key: str,
                 client: TinymanClient,
                 trade_logger: TradeLogger,
                 asset1_id: int
                 ):
        self.client = client
        self.trade_logger = trade_logger
        self.private_key = private_key
        self.address = address
        self.asset1_id = asset1_id
        assert self.asset1_id > 0

        self.logger = logging.getLogger(f'{__name__} {self.asset1_id}')

    def attempt_transaction(self, quote: SwapQuote) -> Optional[AlgoPoolSwap]:
        raise NotImplementedError
