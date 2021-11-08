import pandas as pd
from tinydata import TinyData
from assets import assets, get_name
from time import perf_counter
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple
import collections


@contextmanager
def catchtime() -> float:
    start = perf_counter()
    yield lambda: perf_counter() - start


class SlowData:
    total_usd_reserves: float
    supply: float
    circulating_supply: float
    decimals: float
    volatility: float
    change24h: float
    transactions: float

    price_history: pd.Series

    def __init__(self, price, summary):
        for key in ['total_usd_reserves', 'supply', 'circulating_supply',
                    'decimals', 'volatility', 'change24h', 'transactions']:
            setattr(self, key, summary[key])

        self.price_history = price


class FastData:
    price_last: float
    price1h_last: float

    def __init__(self, instant_prices):
        self.price_last = instant_prices['price']
        self.price1h_last = instant_prices['price1h']


@dataclass
class AssetData:
    fast: FastData
    slow: SlowData


class DataStore(collections.Mapping):
    """Up-to-date prices of all main coins and key metrics"""

    def __init__(self):
        """Set up the data store"""
        self.data: Dict[int, AssetData] = {}
        self.scraper = TinyData()
        self.update()

    def update_fast(self):
        assert len(self.data) > 0
        fastData = self.get_fast_data()
        for asset_id, data in fastData.items():
            self.data[asset_id].fast = data

    def update(self):

        fastData = self.get_fast_data()

        with catchtime() as t:
            for asset_id in assets:
                n, _ = get_name(asset_id)
                price = self.scraper.processed_price_data(asset_id)
                summary = self.scraper.asset_summary(asset_id)

                slow = SlowData(price=price, summary=summary)
                self.data[asset_id] = AssetData(slow=slow, fast=fastData[asset_id])

        # TODO Use logger instead of this
        print(f'Updated slow price data in {t():.4f} seconds.')

    def get_fast_data(self):
        fastData = {}

        with catchtime() as t:
            instant_prices = self.scraper.all_prices()

        for asset_id in assets:
            fastData[asset_id] = FastData(instant_prices['assets'][str(asset_id)])

        print(f'Returned fast price data in {t():.4f} seconds.')
        return fastData

    def __getitem__(self, asset_id):
        return self.data[asset_id]

    def __len__(self):
        return len(self.data)

    def __iter__(self) -> Iterable[Tuple[int, AssetData]]:
        return iter(self.data)
