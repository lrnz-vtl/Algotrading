import unittest
import logging
from algo.blockchain.process_volumes import SwapScraper
from algo.blockchain.process_prices import query_pool_state_history
from tinyman.v1.client import TinymanMainnetClient


class TestData(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestData")

        super().__init__(*args, **kwargs)

    def test_volumes(self, n_queries=10):
        asset1 = 0
        asset2 = 470842789

        sc = SwapScraper(asset1, asset2)
        for tx in sc.scrape(num_queries=n_queries):
            print(tx)

    def test_prices(self, n_queries=10):
        asset1 = 0
        asset2 = 470842789

        client = TinymanMainnetClient()
        pool = client.fetch_pool(asset1, asset2)
        assert pool.exists
        address = pool.address

        for tx in query_pool_state_history(address, n_queries):
            print(tx)
