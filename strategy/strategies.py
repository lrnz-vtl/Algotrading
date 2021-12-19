from abc import ABC, abstractmethod
from swapper import Swapper
from wallets import Portfolio
from strategy.analysis import AnalyticProvider

# class Strategy(ABC):
#     @abstractmethod
#     def assign_portfolio(self, ds, pf):
#         pass
    
#     @abstractmethod
#     def rebalance_portfolio(self, ds, pf):
#         pass

class SimpleStrategyEMA:
    """Simple strategy based on monitoring of EMA"""

    def __init__(self, analytic_provider):
        self.analytic_provider = analytic_provider
        # use a dictionary with {assetid: weight} format for more sophisticated strategies?
        self.buy = set()
        self.sell = set()

    def score(self, assetid):
        """Return list of tuples with coins to long"""
        #for assetid in self.analytic_provider.expavg_long:
        diff = self.analytic_provider.expavg_long[assetid]-self.analytic_provider.expavg_short[assetid]
        # if difference becomes negative and was previously positive, buy coin, and vice vera
        if (diff[-1]<0):
            self.sell.discard(assetid)
            self.buy.add(assetid)
        if (diff[-1]>0):
            self.buy.discard(assetid)
            self.sell.add(assetid)
