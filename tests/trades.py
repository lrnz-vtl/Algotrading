import unittest
import logging
from swapper import Swapper
import sys
from tests.key import get_private_key, address
from trade_logger.text import TextLogger


class TestSwapper(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logger = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        key = get_private_key()

        trade_logger = TextLogger("/home/lorenzo/log.txt")

        self.swapper = Swapper(logger=logger, tradeLogger=trade_logger, address=address, private_key=key)
        super().__init__()

    def test_quote(self):
        print('test')
        pass
