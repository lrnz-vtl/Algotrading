import logging
import aiohttp
from algo.blockchain.stream import PriceVolumeStream, PriceVolumeDataStore
from algo.strategy.analytics import process_market_df
import time
import unittest
from algo.universe.universe import SimpleUniverse
from algo.blockchain.process_volumes import SwapScraper
from algo.blockchain.process_prices import PriceScraper
from algo.blockchain.utils import datetime_to_int
from tinyman.v1.client import TinymanMainnetClient
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient
import datetime
import asyncio


class TestData(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestData")
        self.client = TinymanMainnetClient()

        self.date_min = datetime.datetime(year=2022, month=1, day=20)

        super().__init__(*args, **kwargs)

    def test_volumes(self, n_queries=10):
        asset1 = 0
        asset2 = 470842789

        sc = SwapScraper(self.client, asset1, asset2)

        async def main():
            async with aiohttp.ClientSession() as session:
                async for tx in sc.scrape(session, datetime_to_int(self.date_min), num_queries=n_queries,
                                          before_time=None):
                    print(tx)

        asyncio.run(main())

    def test_prices(self, n_queries=10):
        asset1 = 0
        asset2 = 470842789

        pool = self.client.fetch_pool(asset1, asset2)
        assert pool.exists

        ps = PriceScraper(self.client, asset1, asset2)

        async def main():
            async with aiohttp.ClientSession() as session:
                async for tx in ps.scrape(session, datetime_to_int(self.date_min), num_queries=n_queries,
                                          before_time=None):
                    print(tx)

        asyncio.run(main())

    def test_old_prices(self, n_queries=10):
        asset1 = 0
        # Yieldly
        asset2 = 226701642

        client = TinymanOldnetClient()

        ps = PriceScraper(client, asset1, asset2)

        async def main():
            async with aiohttp.ClientSession() as session:
                async for tx in ps.scrape(session, 0, num_queries=n_queries, before_time=None):
                    print(tx)

        asyncio.run(main())


class TestStream(unittest.TestCase):

    def __init__(self, *args, **kwargs):

        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=logging.DEBUG)

        super().__init__(*args, **kwargs)

    def test_stream(self):
        blockid = 19241100
        universe = SimpleUniverse.from_cache('liquid_algo_pools_nousd_prehack')

        pvs = PriceVolumeDataStore(blockid, universe)
        ti = time.time()
        pvs.scrape()
        print(f'Scraped data since block {blockid} in {time.time() - ti} seconds.')
        for i in range(10):
            time.sleep(1)
            ti = time.time()
            pvs.scrape()
            print(f'Scraped 1 seconds of data in {time.time() - ti} seconds.')

        prices = pvs.prices()
        volumes = pvs.volumes()

        # remove pools without algo
        prices = prices[prices['asset2'] == 0]
        volumes = volumes[volumes['asset2'] == 0]

        market_data = process_market_df(prices, volumes)
        print(market_data)
