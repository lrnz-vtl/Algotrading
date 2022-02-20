from __future__ import annotations
import datetime
import unittest
import logging
from algo.trading.impact import ASAImpactState, PositionAndImpactState, GlobalPositionAndImpactState, \
    ASAPosition
from algo.trading.signalprovider import DummySignalProvider
from algo.blockchain.stream import PoolState, PriceUpdate, stream_from_price_df
from algo.universe.universe import SimpleUniverse
from dataclasses import dataclass
from datetime import timezone
from algo.blockchain.utils import load_algo_pools, make_filter_from_universe
from algo.simulation.simulator import Simulator
from algo.universe.pools import PoolId
from algo.blockchain.utils import datetime_to_int
from algo.trading.trades import TradeInfo


@dataclass
class SimDebugParameter:
    price: float
    impact_decay_seconds: int


def debug_trades(params: SimDebugParameter):
    log_null_trades = False

    asset1_id = 1
    asset2_reserves = 10 ** 12
    frac_pool = 0.1
    risk_coef = 0.000000000001
    impact_timescale_seconds = params.impact_decay_seconds

    simulation_step_seconds = 5 * 60
    initial_mualgo_position = 1000000
    universe = SimpleUniverse(pools=[PoolId(asset1_id, 0, "dummy")])
    seed_time = datetime.timedelta(minutes=4)
    initial_time = datetime.datetime(year=2021, month=11, day=10, tzinfo=timezone.utc)
    end_time = datetime.datetime(year=2021, month=11, day=10, minute=30, tzinfo=timezone.utc)
    asset_ids = [pool.asset1_id for pool in universe.pools]
    assert all(pool.asset2_id == 0 for pool in universe.pools)

    logged_trades: list[TradeInfo] = []

    asset1_reserves = int(asset2_reserves / params.price)

    def price_stream():
        for i in range(4):
            lag_seconds = 1 + i * (60 * 5)
            yield PriceUpdate(asset_ids=(asset1_id, 0),
                              price_update=PoolState(time=datetime_to_int(initial_time) + lag_seconds,
                                                     asset1_reserves=asset1_reserves,
                                                     asset2_reserves=asset2_reserves,
                                                     reverse_order_in_block=0)
                              )

    pos_impact_states = {
        asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds),
                                         asa_position=ASAPosition(int(frac_pool * asset1_reserves)))
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
                          price_stream=price_stream(),
                          log_null_trades=log_null_trades
                          )

    def log_trade(x):
        logged_trades.append(x)

    def log_state(x):
        pass

    simulator.run(end_time, log_trade, log_state)

    return logged_trades


class TestSimulator(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_simulator(self):
        log_null_trades = True

        initial_position_multiplier = 1 / 100

        risk_coef = 0.0000001
        # risk_coef = 0.000000001
        price_cache_name = '20220209_prehack'
        universe_cache_name = 'liquid_algo_pools_nousd_prehack'
        impact_timescale_seconds = 5 * 60
        simulation_step_seconds = 5 * 60
        initial_mualgo_position = 1000000
        seed_time = datetime.timedelta(days=1)
        initial_time = datetime.datetime(year=2021, month=11, day=10, tzinfo=timezone.utc)
        end_time = datetime.datetime(year=2021, month=11, day=20, tzinfo=timezone.utc)

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

        def log_trade(x):
            self.logger.info(x)

        def log_state(x):
            pass

        simulator.run(end_time, log_trade, log_state)

    def test_price_invariance(self):

        def are_trades_equal(param0: SimDebugParameter, param1: SimDebugParameter) -> bool:
            try:
                trades = [debug_trades(param) for param in (param0, param1)]
                assert len(trades[0]) == len(trades[1])
                assert len(trades[0]) > 1, f"len(trades[0]) = {len(trades[0])}"
                for t0, t1 in zip(trades[0], trades[1]):
                    assert t0.price_covariant(t1), f"\n{t0}, \n{t0}"
            except AssertionError:
                return False
            return True

        assert are_trades_equal(SimDebugParameter(0.14, 5 * 60), SimDebugParameter(57, 5 * 60))

        assert not are_trades_equal(SimDebugParameter(0.14, 5 * 60), SimDebugParameter(57, 1 * 60))
