#!/usr/bin/env python3

import asyncio
import logging
from daemon import runner

from tinychart_data.datastore import DataStore
from assets import assets
from strategy.strategies import SimpleStrategyEMA
from strategy.analysis import AnalyticProvider
from trading.tradingengine import TradingEngine
from wallets import Portfolio
from swapper import Swapper
from trade_logger.text import TextLogger
import assets

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

async def run_trading(engine):
    print(f'Running trading engine every {engine.trading_scale/60:.2f} min.')
    while True:
        engine.trading_step()
        await asynchio.sleep(engine.trading_scale)
# async def run_strategy(strat, ds, pf):
#     print('Running strategies every 10 sec.')
#     while True:
#         strat.assign_portfolio(ds, pf)
#         strat.rebalance_portfolio(ds, pf)
#         await asyncio.sleep(10)

class App():
    def __init__(self, address, private_key, testnet=False):
        self.logger = logging.getLogger('trading')
        self.testnet = testnet
        self.swapper = Swapper(address, private_key, TextLogger('trade_logger'), self.logger, False)
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path =  '/dev/tty'
        self.pidfile_path =  '/tmp/foo.pid'
        self.pidfile_timeout = 5
        
    def run(self):
        ds = DataStore()
        ap = AnalyticProvider(ds, 10000, 1000)
        strat = SimpleStrategyEMA(ap)
        pf = Portfolio(address, self.testnet)
        te = TradingEngine(strat, pf, self.swapper, assets.assets)
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(update_fast(ds)),
            loop.create_task(update_slow(ds)),
            loop.create_task(run_trading(te)),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
private_key='Bjf++sgdvSP8y1pSgzyfjYwKmROwYmsRh2+ipE0UF6sTsAHCfBeCZ5okgUFOqqOu7ilPFCTlDUz24cDqd73D9Q=='
address='COYADQT4C6BGPGREQFAU5KVDV3XCSTYUETSQ2THW4HAOU555YP2S3AHALE'

app = App(address, private_key)
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()
