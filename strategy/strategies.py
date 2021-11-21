from abc import ABC, abstractmethod
from swapper import Swapper
from wallets import Portfolio
from strategy.analysis import AnalyticProvider

class Strategy(ABC):
    @abstractmethod
    def assign_portfolio(self, ds, pf):
        pass
    
    @abstractmethod
    def rebalance_portfolio(self, ds, pf):
        pass

class SimpleStrategyEMA(Strategy):
    """Simple strategy based on monitoring of EMA"""

    def __init__(self, analytic_provider, portfolio, time_long=10000, time_short=1000):
        self.analytic_provider = AnalyticProvider(time_long, time_short)
        self.portfolio = portfolio

    def assign_portfolio(self, pf):
        """Return list of tuples with coins to long"""
        pass
    
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
