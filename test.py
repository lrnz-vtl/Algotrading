#!/usr/bin/env python3

import time, asyncio
from daemon import runner

from datastore import DataStore
from assets import assets
from strategies import StrategyStupid, Portfolio

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

async def run_strategy(strat, ds, pf):
    print('Running strategies every 10 sec.')
    while True:
        strat.assign_portfolio(ds, pf)
        strat.rebalance_portfolio(ds, pf)
        await asyncio.sleep(10)

class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path =  '/dev/tty'
        self.pidfile_path =  '/tmp/foo.pid'
        self.pidfile_timeout = 5
    def run(self):
        ds = DataStore()
        strat = StrategyStupid(0.8, 0.4)
        pf = Portfolio(10, assets) # value needs to be loaded from wallet
        
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(update_fast(ds)),
            loop.create_task(update_slow(ds)),
            loop.create_task(run_strategy(strat, ds, pf)),
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

app = App()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()
