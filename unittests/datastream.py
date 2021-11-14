import unittest
import logging
from stream.marketstream import PoolStream, MultiPoolStream
from tinyman.v1.client import TinymanMainnetClient
import asyncio
from asyncio.exceptions import TimeoutError


class TestStream(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestStream")
        super().__init__(*args, **kwargs)

    def test_pool(self):
        asset1 = 0
        asset2 = 226701642

        client = TinymanMainnetClient()

        poolStream = PoolStream(asset1=asset1, asset2=asset2, client=client, log_interval=5, sample_interval=1)

        async def run():
            async def foo():
                async for x in poolStream.run():
                    self.logger.info(x)

            try:
                await asyncio.wait_for(foo(), timeout=11)
            except TimeoutError:
                pass

        asyncio.run(run())

    def test_pools(self):
        assetPairs = [
            (0, 226701642),
            (0, 27165954)
        ]

        client = TinymanMainnetClient()

        multiPoolStream = MultiPoolStream(assetPairs=assetPairs, client=client,
                                          sample_interval=1, log_interval=5, timeout=11)

        async def run():
            async def foo():
                async for x in multiPoolStream.run():
                    self.logger.info(x)
            try:
                await asyncio.wait_for(foo(), timeout=11)
            except TimeoutError:
                pass

        asyncio.run(run())
