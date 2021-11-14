import unittest
import logging
from swapper import Swapper
from unittests.key import get_private_key, address
from trade_logger.text import TextLogger
from wallets import get_account_data, Portfolio
from tinyman.v1.client import TinymanTestnetClient, TinymanMainnetClient


class TestSwapper(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("testSwapper")
        super().__init__(*args, **kwargs)

    def test_quote(self):
        quantity = 1000
        target_price = 0.085
        asset1 = 0
        asset2 = 10458941

        key = get_private_key()
        trade_logger = TextLogger("/home/lorenzo/log.txt")

        self.swapper = Swapper(logger=self.logger, tradeLogger=trade_logger, address=address, private_key=key,
                               testnet=True)
        self.swapper.swap(asset1, asset2, quantity, target_price)

    def test_account_data(self):
        coins = get_account_data(address=address, testnet=True)
        self.logger.info(coins)

    def test_portfolio(self):
        Portfolio(address=address, testnet=True)

    def test_pool(self):
        asset1 = 0
        asset2 = 10458941
        client = TinymanTestnetClient()
        pool = client.fetch_pool(asset2, asset1)
        state = pool.fetch_state()
        self.logger.info(pool)
