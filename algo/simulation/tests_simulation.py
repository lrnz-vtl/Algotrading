from __future__ import annotations
import datetime
import unittest
from algo.trading.impact import ASAImpactState, PositionAndImpactState, GlobalPositionAndImpactState, \
    ASAPosition
from algo.trading.signalprovider import DummySignalProvider, EmaSignalProvider, \
    EmaSignalParam, PriceSignalProvider, RandomSignalProvider
from algo.blockchain.stream import stream_from_price_df
from datetime import timezone
from algo.universe.universe import SimpleUniverse
from algo.blockchain.utils import load_algo_pools, make_filter_from_universe
from algo.simulation.simulator import Simulator
import logging
import pickle
from algo.simulation.simulation import make_simulation_results, make_simulation_reports
from typing import Callable


def test_signal(make_signal: Callable[[], PriceSignalProvider]):
    log_null_trades = True
    risk_coef = 0.000002 * 10 ** -6
    price_cache_name = '20220209_prehack'
    universe_cache_name = 'liquid_algo_pools_nousd_prehack'
    impact_timescale_seconds = 5 * 60
    simulation_step_seconds = 5 * 60

    initial_mualgo_position = 1000 * 10 ** 6

    seed_time = datetime.timedelta(days=1)
    initial_time = datetime.datetime(year=2021, month=10, day=15, tzinfo=timezone.utc)
    end_time = datetime.datetime(year=2021, month=12, day=31, tzinfo=timezone.utc)

    universe = SimpleUniverse.from_cache(universe_cache_name)

    filter_pair = make_filter_from_universe(universe)
    dfp = load_algo_pools(price_cache_name, 'prices', filter_pair)

    price_stream = stream_from_price_df(dfp, initial_time)
    asset_ids = [pool.asset1_id for pool in universe.pools]
    assert all(pool.asset2_id == 0 for pool in universe.pools)

    pos_impact_states = {
        asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds),
                                         ASAPosition(0))
        for asset_id in asset_ids
    }
    pos_impact_state = GlobalPositionAndImpactState(pos_impact_states, initial_mualgo_position)

    signal_providers = {
        asset_id: make_signal() for asset_id in asset_ids
    }

    simulator = Simulator(universe=universe,
                          pos_impact_state=pos_impact_state,
                          signal_providers=signal_providers,
                          simulation_step_seconds=simulation_step_seconds,
                          risk_coef=risk_coef,
                          seed_time=seed_time,
                          price_stream=price_stream,
                          log_null_trades=log_null_trades
                          )

    results = make_simulation_results(simulator, end_time)

    # with open('simResults.pickle', 'wb') as f:
    #     pickle.dump(results, f)

    make_simulation_reports(results)


class TestReports(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_liquidation(self):
        log_null_trades = True
        initial_position_multiplier = 1 / 100
        risk_coef = 0.000002 * 10 ** -6
        price_cache_name = '20220209_prehack'
        universe_cache_name = 'liquid_algo_pools_nousd_prehack'
        impact_timescale_seconds = 5 * 60
        simulation_step_seconds = 5 * 60
        initial_mualgo_position = 1000000
        seed_time = datetime.timedelta(days=1)
        initial_time = datetime.datetime(year=2021, month=11, day=10, tzinfo=timezone.utc)

        universe = SimpleUniverse.from_cache(universe_cache_name)

        filter_pair = make_filter_from_universe(universe)
        dfp = load_algo_pools(price_cache_name, 'prices', filter_pair)

        # Just choose some starting positions
        initial_positions = (dfp.groupby('asset1')['asset1_reserves'].mean() * initial_position_multiplier).astype(int)

        price_stream = stream_from_price_df(dfp, initial_time)
        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        pos_impact_states = {
            asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds),
                                             ASAPosition(int(initial_positions.loc[asset_id])))
            for asset_id in asset_ids
        }
        pos_impact_state = GlobalPositionAndImpactState(pos_impact_states, initial_mualgo_position)

        signal_providers = {
            asset_id: DummySignalProvider() for asset_id in asset_ids
        }

        simulator = Simulator(universe=universe,
                              pos_impact_state=pos_impact_state,
                              signal_providers=signal_providers,
                              simulation_step_seconds=simulation_step_seconds,
                              risk_coef=risk_coef,
                              seed_time=seed_time,
                              price_stream=price_stream,
                              log_null_trades=log_null_trades
                              )

        end_time = datetime.datetime(year=2021, month=11, day=20, tzinfo=timezone.utc)

        results = make_simulation_results(simulator, end_time)

        make_simulation_reports(results)

    def test_fitted_signal(self):
        params = [
            EmaSignalParam(30 * 60, -0.14389164),
            EmaSignalParam(60 * 60, 0.14088184),
            EmaSignalParam(120 * 60, -0.12424874)
        ]

        def make_signal():
            return EmaSignalProvider(params)

        test_signal(make_signal)

    def test_random_signal(self):

        def make_signal():
            return RandomSignalProvider(0.002)

        test_signal(make_signal)
