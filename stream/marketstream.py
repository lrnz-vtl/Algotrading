from datetime import datetime
from dataclasses import dataclass
from tinyman.v1.pools import Pool
from tinyman.v1.client import TinymanClient
from utils.timestamp import Timestamp
from datetime import timezone, timedelta
from typing import Optional, Tuple, Iterable, Generator
import asyncio
from aiostream import stream
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


def aggregatePrice(bucket_delta: int = 60 * 5) -> Generator[Tuple[datetime, float], Tuple[Timestamp, Pool], None]:
    """
    Very basic time-average price aggregator
    """

    time: Optional[Timestamp] = None
    bucket_delta = bucket_delta
    mean = None

    while True:
        t, pool = (yield)

        # Price of buying infinitesimal amount of asset2 in units of asset1, excluding transaction costs
        price = pool.asset1_reserves / pool.asset2_reserves

        if mean is not None:
            t0 = time_bucket(time.utcnow, bucket_delta)
            t1 = time_bucket(t.utcnow, bucket_delta)

            if t0 != t1:
                yield t0, mean.value()
                mean = RunningMean()
        else:
            mean = RunningMean()

        mean.add(price)
        time = t


class PoolStream:

    def __init__(self, asset1, asset2, client: TinymanClient,
                 sample_interval: int = 5,
                 log_interval: int = 60):

        self.aggregate = aggregatePrice(log_interval)
        self.sample_interval = sample_interval
        self.asset1 = asset1
        self.asset2 = asset2
        self.client = client

    async def run(self):
        next(self.aggregate)

        while True:
            pool = self.client.fetch_pool(self.asset1, self.asset2)
            time = Timestamp.get()

            row = self.aggregate.send((time,pool))

            if row:
                yield row

            await asyncio.sleep(self.sample_interval)


class MultiPoolStream:

    def __init__(self, assetPairs: Iterable[Tuple[int, int]], client: TinymanClient,
                 timeout: Optional[int],
                 sample_interval: int = 5,
                 log_interval: int = 60 * 5):

        self.timeout = timeout
        self.assetPairs = assetPairs
        self.poolStreams = [
                PoolStream(asset1=pair[0], asset2=pair[1], client=client, sample_interval=sample_interval,
                           log_interval=log_interval) for pair in assetPairs
        ]

    async def run(self):

        async def withPairInfo(assetPair, poolStream):
            async for x in poolStream.run():
                yield assetPair, x

        async_generators = [withPairInfo(assetPair, poolStream) for (assetPair, poolStream) in zip(self.assetPairs, self.poolStreams)]

        combine = stream.merge(*async_generators)

        async with combine.stream() as streamer:
            async for row in streamer:
                yield row