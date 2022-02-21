from __future__ import annotations
import unittest
from algo.trading.impact import ASAImpactState, PositionAndImpactState, GlobalPositionAndImpactState, \
    ASAPosition
from algo.trading.signalprovider import EmaSignalProvider, EmaSignalParam, PriceSignalProvider
from algo.universe.universe import SimpleUniverse
from algo.optimizer.optimizerV2 import OptimizerV2
from algo.blockchain.mixedstream import MixedPriceStreamer
from algo.trading.swapper import SimulationSwapper, RedeemedAmounts
import logging
from typing import Callable
from algo.engine.engine import Engine
from tinyman.v1.client import TinymanMainnetClient
import datetime


class TestEngine(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.optimizer_cls = OptimizerV2

        super().__init__(*args, **kwargs)

    def _test_engine(self, make_signal: Callable[[], PriceSignalProvider]):
        risk_coef = 0.000002 * 10 ** -6

        impact_timescale_seconds = 5 * 60

        trading_step_seconds = 10
        marketupdate_step_seconds = 2
        syncpositions_step_seconds = 100

        initial_mualgo_position = 1000 * 10 ** 6

        universe_cache_name = 'liquid_algo_pools_nousd_prehack'
        universe = SimpleUniverse.from_cache(universe_cache_name)

        client = TinymanMainnetClient()
        utcnow = datetime.datetime.utcnow()
        date_min = utcnow.replace(hour=0, minute=0, second=0, microsecond=0)

        mps = MixedPriceStreamer(universe, date_min, client)

        def stream_price():
            yield from mps.scrape()

        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        pos_impact_states = {
            asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds),
                                             ASAPosition(0))
            for asset_id in asset_ids
        }
        pos_impact_state = GlobalPositionAndImpactState(pos_impact_states, initial_mualgo_position)

        engine = Engine(universe=universe,
                        price_scraper=stream_price,
                        trading_step_seconds=trading_step_seconds,
                        marketupdate_step_seconds=marketupdate_step_seconds,
                        syncpositions_step_seconds=syncpositions_step_seconds,
                        risk_coef=risk_coef,
                        optimizer_cls=OptimizerV2,
                        make_swapper=lambda aid: SimulationSwapper(),
                        make_signal_provider=lambda aid: make_signal(),
                        slippage=0
                        )
        engine.pos_impact_state = pos_impact_state

        engine.run(lambda x: self.logger.info(x), lambda x: None)

    def test_fitted_signal_2h_cap(self):
        minutes = (30, 60, 120)
        betas = [-0.31642892, 0.45522962, -0.38743692]

        params = [EmaSignalParam(minute * 60, beta) for minute, beta in zip(minutes, betas)]

        def make_signal():
            return EmaSignalProvider(params, 0.05)

        self._test_engine(make_signal)
