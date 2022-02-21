from __future__ import annotations
import datetime
import logging
import unittest

from algo.trading.trades import TradeInfo
from algo.trading.impact import GlobalPositionAndImpactState, StateLog
from algo.optimizer.base import BaseOptimizer
from algo.trading.signalprovider import PriceSignalProvider
from algo.blockchain.stream import PoolState, PriceUpdate
from algo.universe.universe import SimpleUniverse
from typing import Callable, Generator, Any, Type
from algo.blockchain.utils import int_to_tzaware_utc_datetime
from algo.trading.swapper import ProductionSwapper, TimedSwapQuote, MaybeTradedSwap, Swapper
from tinyman.v1.client import TinymanClient
from algo.engine.base import BaseEngine, lag_ms
from tinyman.v1.pools import SwapQuote
import asyncio
import datetime
import requests


class Engine(BaseEngine):

    def current_time_prov(self) -> datetime.datetime:
        return datetime.datetime.utcnow()

    def __init__(self,
                 universe: SimpleUniverse,
                 price_scraper: Callable[[], Generator[PriceUpdate, Any, Any]],
                 trading_step_seconds: int,
                 marketupdate_step_seconds: int,
                 syncpositions_step_seconds: int,
                 risk_coef: float,
                 optimizer_cls: Type[BaseOptimizer],
                 make_swapper: Callable[[int], Swapper],
                 make_signal_provider: Callable[[int], PriceSignalProvider],
                 slippage: float
                 ):

        self.slippage = slippage
        self.asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)
        self.optimizers: dict[int, BaseOptimizer] = {asset_id: optimizer_cls.make(asset1=asset_id, risk_coef=risk_coef)
                                                     for asset_id in self.asset_ids}
        self.logger = logging.getLogger(__name__)

        self.trading_step_seconds = trading_step_seconds
        self.marketupdate_step_seconds = marketupdate_step_seconds
        self.syncpositions_step_seconds = syncpositions_step_seconds

        self.price_scraper = price_scraper

        self.prices: dict[int, PoolState] = {}
        self.signal_providers = {aid: make_signal_provider(aid) for aid in self.asset_ids}
        self.last_update_times: dict[int, datetime.datetime] = {}

        self.swapper = {aid: make_swapper(aid) for aid in self.asset_ids}

        self.pos_impact_state: GlobalPositionAndImpactState = None

    def sync_market_state(self) -> None:
        start_time = datetime.datetime.utcnow()
        min_market_time = None
        max_market_time = None

        try:
            for x in self.price_scraper():
                assert x.asset_ids[1] == 0
                assert x.asset_ids[0] in self.asset_ids
                asset_id, price_update = x.asset_ids[0], x.price_update

                # Time of the price update
                # time = int_to_tzaware_utc_datetime(x.price_update.time)
                time = datetime.datetime.utcfromtimestamp(x.price_update.time)

                if min_market_time is None:
                    min_market_time = time
                else:
                    assert time >= max_market_time
                max_market_time = time

                if asset_id in self.last_update_times:
                    assert time >= self.last_update_times[asset_id]
                self.last_update_times[asset_id] = time
                self.prices[asset_id] = price_update
                self.signal_providers[asset_id].update(time,
                                                       price_update.asset2_reserves / price_update.asset1_reserves)

            end_time = datetime.datetime.utcnow()
            dt_run = lag_ms(end_time - start_time)
            if max_market_time is not None:
                dt_market = lag_ms(max_market_time - min_market_time)
            else:
                dt_market = 0
            self.logger.debug(f'Scraped {dt_market} ms worth of market data in {dt_run} ms')

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f'Price scraping in sync_market_state failed with ConnectionError: {e}')

    def sync_positions_and_redeem(self) -> None:
        time = self.current_time_prov()
        self.logger.info(f'Entering sync position loop at time {time}')

        for aid in self.asset_ids:
            swapper = self.swapper[aid].fetch_excess_amounts()

    def run(self,
            log_trade: Callable[[TradeInfo], None],
            log_state: Callable[[StateLog], None]):

        self.logger.info('Syncing market state at start')
        start = self.current_time_prov()
        self.sync_market_state()
        end = self.current_time_prov()
        dt = lag_ms(end - start)
        self.logger.info(f'Synced market state at start in {dt} ms')

        async def trade():
            while True:
                self.trade_loop(log_trade, log_state)
                await asyncio.sleep(self.trading_step_seconds)

        async def market_update():
            while True:
                self.sync_market_state()
                await asyncio.sleep(self.marketupdate_step_seconds)

        async def sync_positions():
            while True:
                self.sync_positions_and_redeem()
                await asyncio.sleep(self.syncpositions_step_seconds)

        async def run():
            await asyncio.gather(market_update(), trade(), sync_positions())

        asyncio.run(run())
