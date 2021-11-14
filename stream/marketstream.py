from tinyman.v1.client import TinymanClient
from utils.timestamp import Timestamp
from typing import Optional, Tuple, Iterable, AsyncGenerator, Coroutine, Any
import asyncio
from aiostream import stream
from stream.aggregators import aggregatePrice
from asyncio.exceptions import TimeoutError


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

            row = self.aggregate.send((time, pool))

            if row:
                yield row

            await asyncio.sleep(self.sample_interval)


class MultiPoolStream:

    def __init__(self, assetPairs: Iterable[Tuple[int, int]], client: TinymanClient,
                 sample_interval: int = 5,
                 log_interval: int = 60 * 5):

        self.assetPairs = assetPairs
        self.poolStreams = [
            PoolStream(asset1=pair[0], asset2=pair[1], client=client, sample_interval=sample_interval,
                       log_interval=log_interval) for pair in assetPairs
        ]

    async def run(self):

        async def withPairInfo(assetPair, poolStream):
            async for x in poolStream.run():
                yield assetPair, x

        async_generators = [withPairInfo(assetPair, poolStream) for (assetPair, poolStream) in
                            zip(self.assetPairs, self.poolStreams)]

        combine = stream.merge(*async_generators)

        async with combine.stream() as streamer:
            async for row in streamer:
                yield row


def log_stream(async_gen: AsyncGenerator, timeout: Optional[int], logger_fun) -> Coroutine[Any, Any, None]:
    async def run():
        async def foo():
            async for x in async_gen:
                logger_fun(x)

        try:
            await asyncio.wait_for(foo(), timeout=timeout)
        except TimeoutError:
            pass

    return run()