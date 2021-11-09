from abc import ABC, abstractmethod
from swapper import Swapper
from wallets import Portfolio        
        
class Strategy(ABC):
    @abstractmethod
    def assign_portfolio(self, ds, pf):
        pass
    
    @abstractmethod
    def rebalance_portfolio(self, ds, pf):
        pass

class StrategyStupid(Strategy):
    """Find asset with largest price drop in last hour"""

    def __init__(self, stoploss, maxswap):
        self.stoploss = stoploss
        self.maxswap = maxswap

    def assign_portfolio(self, ds, pf):
        """Return list of tuples with coins to long"""
        pass
        # # find coin whose priced dropped the most in past hour
        # asset = min(ds.data, key = lambda d : ds[d].fast.price_last-ds[d].fast.price1h_last)
        # # YOLO into it
        # pf.swap(0, asset, self.available_coins[0]*self.maxswap, ds[asset].fast.price_last)
    
    def rebalance_portfolio(self, ds, pf):
        """Sell of coins that have been positive in the past hour"""
        pass
        # for asset in data:
        #     if self.available_coins[asset]==0:
        #         continue
        #     if (ds[asset].fast.price_last > ds[asset].fast.price1h_last):
        #         pf.swap(asset, 0, self.available_coins[asset], 1/ds[asset].fast.price_last)
        #     # cut our losses if it dropped too much
        #     if (ds[asset].fast.price_last < self.stoploss*self.average_price[asset]):
        #         pf.swap(asset, 0, self.available_coins[asset], 1/ds[asset].fast.price_last)
