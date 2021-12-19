
class TradingEngine:
    def __init__(self, strategy, portfolio, swapper, asset_ids, trading_scale=15):
        self.portfolio = portfolio
        self.budget = portfolio[0]/len(asset_ids)
        self.strategy = strategy
        self.swapper = swapper
        self.asset_ids = asset_ids
        self.trading_scale = trading_scale # 5 min by default

    async def run_trading(self):
        """Run the main trading engine"""
        print('Running trading engine')
        while True:
            await asyncio.sleep(self.trading_scale)
            self.trading_step()

    def trading_step(self):
        """Evaluate the strategy and perform swaps"""
        print('Performing a trading step')
        for assetid in self.asset_ids:
            self.strategy.score(assetid)
            if assetid in self.strategy.buy:
                if self.portfolio[assetid] == 0:
                    print('Buying asset',assetid)
                    quantity = min(self.portfolio[0], self.budget)
                    self.swapper.swap(0, assetid, quantity, target_price=0.0)
            elif assetid in self.strategy.sell:
                if self.portfolio[assetid] > 0:
                    print('Selling asset',assetid)
                    quantity = self.portfolio[assetid]
                    self.swapper.swap(assetid, 0, quantity, target_price=0.0)
