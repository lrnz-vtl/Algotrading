#!/usr/bin/env python3

import asyncio
import logging
from daemon import runner

from algo.universe.pools import Universe
from algo.strategy.strategies import StrategyArbitrage
from algo.strategy.analysis import PoolGraph
from algo.trading.tradingengine import TradingEngineArbitrage
# FIXME
from keys import address, private_key
from wallets import Portfolio
from swapper import Swapper
from algo.trade_logger.text import TextLogger
from tinyman.v1.client import TinymanMainnetClient, TinymanTestnetClient


async def update_fast(ds):
    print('Running fast updates every 10 sec.')
    while True:
        await asyncio.sleep(10)
        ds.update_fast()


async def update_slow(ds):
    print('Running slow updates every 15 min.')
    while True:
        await asyncio.sleep(900)
        ds.update()


async def update_graph(ap):
    # async def 
    print('Continuously updating graph')
    async for row in ap.mps.run():
        ap.graph[row.asset1][row.asset2]['price'] = row.price
        ap.graph[row.asset1][row.asset2]['asset1_reserves'] = row.asset1_reserves
        ap.graph[row.asset1][row.asset2]['asset2_reserves'] = row.asset2_reserves
        ap.graph[row.asset1][row.asset2]['asset1'] = row.asset1
        ap.graph[row.asset1][row.asset2]['asset2'] = row.asset2
        # print(row)
        await asyncio.sleep(0.05)
    # await ap.run()


async def run_trading(engine):
    print(f'Running trading engine every {engine.trading_scale / 60:.2f} min.')
    while True:
        await asyncio.sleep(engine.trading_scale)
        print('\nStarting trading step\n')
        engine.trading_step()


# async def run_strategy(strat, ds, pf):
#     print('Running strategies every 10 sec.')
#     while True:
#         strat.assign_portfolio(ds, pf)
#         strat.rebalance_portfolio(ds, pf)
#         await asyncio.sleep(10)

class App():
    def __init__(self, address, private_key, analytic_provider, strategy, trading_class, testnet=False):
        self.analytic_provider = analytic_provider
        self.strategy = strategy
        self.trading_engine = trading_class
        self.logger = logging.getLogger('trading')
        self.testnet = testnet
        tradelogger = TextLogger('trade_logs.txt')
        self.swapper = Swapper(address, private_key, tradelogger, self.logger, False)
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/foo.pid'
        self.pidfile_timeout = 5

    def run(self):
        pf = Portfolio(address, self.testnet)
        te = self.trading_engine(self.strategy, pf, 8_000_000, self.swapper)
        loop = asyncio.get_event_loop()
        # tasks = [
        #     loop.create_task(update_graph(self.analytic_provider)),
        #     loop.create_task(run_trading(te)),
        # ]
        # tasks = [
        #     loop.create_task(update_fast(ds)),
        #     loop.create_task(update_slow(ds)),
        #     loop.create_task(run_trading(te)),
        # ]
        # loop.run_until_complete(asyncio.wait(tasks))

        loop.create_task(update_graph(self.analytic_provider))
        loop.create_task(run_trading(te))
        loop.run_forever()
        loop.close()


testnet = False

# ds = DataStore()
# ap = AnalyticProvider(ds, 10000, 1000)
# app = App(address, private_key, strategy_class=SimpleStrategyEMA, trading_class=TradingEngineEMA)
samplingLogger = logging.getLogger("SamplingLogger")

# pools = all_pairs
client = TinymanTestnetClient() if testnet else TinymanMainnetClient()
universe = Universe(client=client, check_pairs=False)
ap = PoolGraph(assetPairs=universe.pairs,
               client=client,
               num_trades=50, logger=samplingLogger, sample_interval=10, log_interval=50)
strat = StrategyArbitrage(ap, gain_threshold=1.012)

print(f'Starting a trading strategy with {len(universe.pairs)} pools')
app = App(address, private_key, analytic_provider=ap, strategy=strat, trading_class=TradingEngineArbitrage)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()
