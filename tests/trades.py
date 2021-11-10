import unittest
import logging
from swapper import Swapper
from tests.key import get_private_key, address
from trade_logger.text import TextLogger


class TestSwapper(unittest.TestCase):

    def __init__(self, *args, **kwargs):

        logging.basicConfig(level=logging.NOTSET)
        logger = logging.getLogger("testSwapper")

        key = get_private_key()

        trade_logger = TextLogger("/home/lorenzo/log.txt")

        self.swapper = Swapper(logger=logger, tradeLogger=trade_logger, address=address, private_key=key, testnet=True)
        super().__init__(*args, **kwargs)

    def test_quote(self):
        quantity = 1000
        target_price = 0.085
        asset1 = 0
        asset2 = 10458941

        self.swapper.swap(asset1, asset2, quantity, target_price)