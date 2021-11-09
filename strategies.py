from abc import ABC, abstractmethod
from swapper import Swapper

class Portfolio:
    def __init__(self, total_funds, assets):
        """Set up the portfolio, starting with 100% Algorand"""
        #self.swapper = Swapper(address, private_key)
        #get data below from the wallet directly
        self.available_funds = total_funds
        self.available_coins = dict.fromkeys(assets, 0)
        self.available_coins[0] = total_funds
        self.average_price = dict.fromkeys(assets)
        self.average_price[0] = 1

    def swap(self, asset_id1, asset_id2, quantity, target_price):
        #if self.swapper(asset_id1, asset_id2, quantity, target_price):
        price = target_price # get from swap not input
        qty1 = quantity # get from swap not input
        qty2 = quantity*price
        #update average price paid for coin
        if asset_id in self.average_price:
            self.average_price[asset_id2] = \
                (self.average_price[asset_id2]*self.available_coins[asset_id2] + qty2*price) \
                / (qty2+self.available_coins[asset_id2])
        else:
            self.average_price[asset_id2] = price
        # update available funds
        self.available_coins[asset_id1]-=qty1
        self.available_coins[asset_id2]+=qty2
        
        
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
        # find coin whose priced dropped the most in past hour
        asset = min(ds.data, key = lambda d : ds[d].fast.price_last-ds[d].fast.price1h_last)
        # YOLO into it
        pf.swap(0, asset, self.available_coins[0]*self.maxswap, ds[asset].fast.price_last)
    
    def rebalance_portfolio(self, ds, pf):
        """Sell of coins that have been positive in the past hour"""
        for asset in data:
            if self.available_coins[asset]==0:
                continue
            if (ds[asset].fast.price_last > ds[asset].fast.price1h_last):
                pf.swap(asset, 0, self.available_coins[asset], 1/ds[asset].fast.price_last)
            # cut our losses if it dropped too much
            if (ds[asset].fast.price_last < self.stoploss*self.average_price[asset]):
                pf.swap(asset, 0, self.available_coins[asset], 1/ds[asset].fast.price_last)
