import unittest
import logging
from stream.marketstream import PoolStream, MultiPoolStream, log_stream
from stream import sqlite
from tinyman.v1.client import TinymanMainnetClient
import asyncio
import uuid
from contextlib import closing
import os


class TestStream(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestStream")

        filename = kwargs.get('filename')
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
        samplingLogger.setLevel(logging.FATAL)

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
        samplingLogger.setLevel(logging.FATAL)

        multiPoolStream = MultiPoolStream(assetPairs=assetPairs, client=client, sample_interval=1, log_interval=5, logger=samplingLogger)

        dbfname = f'/tmp/{str(uuid.uuid4())}.db'

        # def logf(x):
        #     self.logger.info(x)

        with sqlite.MarketSqliteLogger(dbfile=dbfname) as marketLogger:

            marketLogger.create_table()
            logf = lambda x: marketLogger.log(x)

            logger_coroutine = log_stream(multiPoolStream.run(), timeout=11, logger_fun=logf)
            asyncio.run(logger_coroutine)

            with closing(marketLogger.con.cursor()) as c:
                c.execute(f"select * from {marketLogger.tablename}")
                self.logger.info(c.fetchall())

        os.remove(dbfname)
