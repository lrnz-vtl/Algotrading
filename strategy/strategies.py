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

    def __init__(self, analytic_provider, maxfrac = 0.1, time_long=10000, time_short=1000):
        self.analytic_provider = AnalyticProvider(time_long, time_short)
        #self.swapper = Swapper(address, private_key, logger=logging.getLogger("Swapper"), trade_logger=TextLogger("log.txt"))
        #self.portfolio = Portfolio(address)
        self.maxfrac = 0.1
        self.holdings = {}
        
    def rebalance_portfolio(self):
        """Return list of tuples with coins to long"""
        for assetid in self.analytic_provider.expavg_long:
            diff = self.analytic_provider.expavg_long[assetid]-self.analytic_provider.expavg_short[assetid]
            # if difference becomes negative and was previously positive, buy coin, and vice vera
            if (diff[-1]<0):
                if (assetid in self.holdings and self.holdings[assetid]==True):
                    continue
                else:
                    self.buy(assetid)
            if (diff[-1]>0):
                if (assetid in self.holdings and self.holdings[assetid]==False):
                    continue
                else:
                    self.sell(assetid)

    def sell(self, assetid):
        """Sell assetid"""
        #quantity = self.portfolio[assetid]
        #self.swapper.swap(assetid, 0, quantity, 0)
        pass

    def buy(self, assetid):
        """Buy assetid"""
        #quantity = self.portfolio[0]*self.maxfrac
        #self.swapper.swap(0, assetid, quantity, 0)
        pass
