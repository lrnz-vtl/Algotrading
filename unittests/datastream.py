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

    def test_pool(self, timeout=11, sample_interval=1, log_interval=5):
        asset1 = 0
        asset2 = 226701642

        client = TinymanMainnetClient()

        samplingLogger = logging.getLogger("SamplingLogger")
        samplingLogger.setLevel(logging.DEBUG)

        poolStream = PoolStream(asset1=asset1, asset2=asset2, client=client, log_interval=log_interval,
                                sample_interval=sample_interval, logger=samplingLogger)

        def logf(x):
            self.logger.info(x)

        logger_coroutine = log_stream(poolStream.run(), timeout=timeout, logger_fun=logf)
        asyncio.run(logger_coroutine)

    def test_pools(self, timeout=11, sample_interval=1, log_interval=5):
        assetPairs = [
            (0, 226701642),
            (0, 27165954),
            (0, 230946361),
            (0, 287867876),
            (0, 384303832),
            
            (0, 378382099),
            (0, 2751733),
            (0, 300208676),
            (0, 137594422),
            (0, 163650),

            (0, 359487296),
            (0, 400758048),
            (0, 404044168),
            (0, 297995609),
            (0, 361671874),
            
            (0, 137020565),
            (0, 406383570),
            (0, 241759159),
            (0, 400593267),
            (0, 283820866)
        ]

        client = TinymanMainnetClient()

        samplingLogger = logging.getLogger("SamplingLogger")
        samplingLogger.setLevel(logging.DEBUG)

        multiPoolStream = MultiPoolStream(assetPairs=assetPairs, client=client, sample_interval=sample_interval,
                                          log_interval=log_interval, logger=samplingLogger)

        def logf(x):
            self.logger.info(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=timeout, logger_fun=logf)
        asyncio.run(logger_coroutine)
