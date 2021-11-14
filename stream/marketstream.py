from datetime import datetime
from dataclasses import dataclass
from tinyman.v1.pools import Pool
from tinyman.v1.client import TinymanClient
from utils.timestamp import Timestamp
from datetime import tzinfo, timezone, timedelta
from typing import Optional
import asyncio
import numpy as np


@dataclass
class TradeInfo:
    asset1_id: int
    asset2_id: int
    quantity: float
    target_price: float
    slippage: float
    excess_min: float


@dataclass
class PoolState:
    pool: Pool
    now: datetime
    utcnow: datetime


class RunningMean:

    def __init__(self):
        self.x = np.nan
        self.n = 0

    def add(self, x):
        if np.isnan(self.x):
            self.x = 0
        self.x += x
        self.n += 1

    def value(self):
        return self.x / self.n


def time_bucket(utcnow, log_interval):
    time = (utcnow - datetime(1970, 1, 1, tzinfo=timezone.utc))
    delta = timedelta(seconds=time.total_seconds() % log_interval)
    return utcnow - delta


class PriceAggregator:
    """
    Very basic time-average price aggregator
    """

    def __init__(self, log_function, log_interval: int = 60 * 5):
        self.time: Optional[Timestamp] = None
        self.price = None
        self.log_function = log_function
        self.log_interval = log_interval
        self.mean = None

    def log(self, pool: Pool, t: Timestamp):

        # Price of buying infinitesimal amount of asset2 in units of asset1, excluding transaction costs
        price = pool.asset1_reserves / pool.asset2_reserves

        if self.mean is not None:
            t0 = time_bucket(self.time.utcnow, self.log_interval)
            t1 = time_bucket(t.utcnow, self.log_interval)

            if t0 != t1:
                self.log_function((t0, self.mean.value()))
                self.mean = RunningMean()
        else:
            self.mean = RunningMean()

        self.mean.add(price)
        self.time = t


class PoolStream:

    def __init__(self, asset1, asset2, client: TinymanClient,
                 aggregator: PriceAggregator,
                 sample_interval: int = 5,
                 keep_running=lambda: True):
        self.aggregator = aggregator
        self.sample_interval = sample_interval
        self.asset1 = asset1
        self.asset2 = asset2
        self.client = client
        self.keep_running = keep_running

    async def run(self):
        loop = asyncio.get_running_loop()
        while self.keep_running():
            self.sample()
            await asyncio.sleep(1)

    def sample(self):
        pool = self.client.fetch_pool(self.asset1, self.asset2)
        time = Timestamp.get()
        self.aggregator.log(pool, time)
