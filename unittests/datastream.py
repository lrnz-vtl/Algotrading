import unittest
import logging
from stream.marketstream import PoolStream, PriceAggregator
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

        logf = lambda x: self.logger.info(x)

        class Counter:
            def __init__(self):
                self.n = 11

            def keep_running(self):
                self.n -= 1
                if self.n > 0:
                    return True
                return False

        c = Counter()

        aggregator = PriceAggregator(logf, log_interval=5)

        poolStream = PoolStream(asset1=asset1, asset2=asset2, client=client, aggregator=aggregator, sample_interval=1,
                                keep_running=lambda: c.keep_running())
        asyncio.run(poolStream.run())
