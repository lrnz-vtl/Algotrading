import unittest
import logging
from stream.marketstream import PoolStream, MultiPoolStream, log_stream
from tinyman.v1.client import TinymanMainnetClient
import asyncio


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

        def logf(x):
            self.logger.info(x)

        logger_coroutine = log_stream(poolStream.run(), timeout=11, logger_fun=logf)
        asyncio.run(logger_coroutine)

    def test_pools(self):
        assetPairs = [
            (0, 226701642),
            (0, 27165954)
        ]

        client = TinymanMainnetClient()

        multiPoolStream = MultiPoolStream(assetPairs=assetPairs, client=client, sample_interval=1, log_interval=5)

        def logf(x):
            self.logger.info(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=11, logger_fun=logf)
        asyncio.run(logger_coroutine)