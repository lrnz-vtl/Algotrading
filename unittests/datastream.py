import unittest
import logging
from stream.marketstream import PoolStream, MultiPoolStream, log_stream
from tinyman.v1.client import TinymanMainnetClient
import asyncio


class TestStream(unittest.TestCase):

    def __init__(self, filename = None, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestStream")
        if filename:
            fh = logging.FileHandler(filename)
            fh.setLevel(logging.INFO)
            self.logger.addHandler(fh)
        super().__init__(*args, **kwargs)

    def test_pool(self):
        asset1 = 0
        asset2 = 226701642

        client = TinymanMainnetClient()

        samplingLogger = logging.getLogger("SamplingLogger")
        samplingLogger.setLevel(logging.DEBUG)

        poolStream = PoolStream(asset1=asset1, asset2=asset2, client=client, log_interval=5, sample_interval=1, logger=samplingLogger)

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

        samplingLogger = logging.getLogger("SamplingLogger")
        samplingLogger.setLevel(logging.DEBUG)

        multiPoolStream = MultiPoolStream(assetPairs=assetPairs, client=client, sample_interval=1, log_interval=5, logger=samplingLogger)

        def logf(x):
            self.logger.info(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=11, logger_fun=logf)
        asyncio.run(logger_coroutine)
