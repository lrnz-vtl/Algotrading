from __future__ import annotations
from dataclasses import asdict
import pandas as pd
from matplotlib import pyplot as plt
import datetime
import unittest
from algo.trading.impact import ASAImpactState, PositionAndImpactState, GlobalPositionAndImpactState, \
    ASAPosition
from algo.trading.signalprovider import DummySignalProvider
from algo.blockchain.stream import stream_from_price_df
from dataclasses import dataclass
from datetime import timezone
from algo.universe.universe import SimpleUniverse
from algo.blockchain.utils import load_algo_pools, make_filter_from_universe, datetime_to_int
from algo.simulation.simulator import Simulator
import logging
import numpy as np


@dataclass
class SimulationResults:
    trade_df: pd.DataFrame
    state_df: pd.DataFrame
    initial_state: GlobalPositionAndImpactState


def make_simulation_results(simulator: Simulator, end_time: datetime.datetime) -> SimulationResults:
    trade_data = []
    state_data = []

    def log_trade(x):
        trade_data.append(x)

    def log_state(x):
        state_data.append(x)

    simulator.run(end_time, log_trade, log_state)

    trade_df = pd.json_normalize([asdict(obj) for obj in trade_data])
    trade_df['unix_time_seconds'] = trade_df['trade.time'].apply(datetime_to_int)
    trade_df = trade_df.set_index(['trade.asset_sell_id', 'unix_time_seconds']).sort_index()

    rows = []
    for x in state_data:
        rows += [pd.Series(
            {'time': x.time, 'asset_id': aid, 'position': state.asa_position.value, 'impact_bps': state.impact.state})
            for aid, state in x.state.asa_states.items()]
        rows.append(
            pd.Series({'time': x.time, 'asset_id': 0, 'position': x.state.mualgo_position, 'impact_bps': np.nan}))

    state_df = pd.concat(rows, axis=1).transpose()
    state_df['unix_time_seconds'] = state_df['time'].apply(datetime_to_int)
    state_df = state_df.set_index(['asset_id', 'unix_time_seconds']).sort_index()

    return SimulationResults(trade_df=trade_df, state_df=state_df,
                             initial_state=simulator.initial_pos_impact_state)


def make_simulation_reports(sim_results: SimulationResults):
    aids = sim_results.trade_df.index.get_level_values(0).unique()
    cols = len(aids)

    rows = 3

    f, axss = plt.subplots(rows, cols, figsize=(5 * cols, 4 * rows), sharex='col')

    def prepend(s: pd.Series, x):
        return pd.concat([pd.Series([x], index=[s.index.values[0] - 5 * 60]), s])

    def prepend_first(s: pd.Series):
        return prepend(s, s.values[0])

    def prepend_zero(s: pd.Series):
        return prepend(s, 0)

    def index_to_dt(s: pd.Series):
        s.index = pd.to_datetime(s.index, unit='s', utc=True)
        return s

    total_asa_positions_algo = []

    for i, aid in enumerate(aids):

        subdf = sim_results.trade_df.loc[aid]
        subdf_state = sim_results.state_df.loc[aid]

        ax = axss[0][i]
        positions = sim_results.initial_state.asa_states[aid].asa_position.value - prepend_zero(
            subdf['trade.asset_sell_amount']).cumsum()
        positions = index_to_dt(positions)
        positions2 = prepend(subdf_state['position'], sim_results.initial_state.asa_states[aid].asa_position.value)
        positions2 = index_to_dt(positions2)
        positions.plot(ax=ax, label='position')
        positions2.plot(ax=ax, label='position2', ls='--')
        ax.set_title(aid)
        if i == len(aids) - 1:
            ax.legend()
        ax.grid()

        ax = axss[1][i]
        prices = index_to_dt(prepend_first(subdf['asa_price']))
        algo_positions = positions * prices / 10 ** 6
        algo_positions2 = positions2 * prices / 10 ** 6
        total_asa_positions_algo.append(algo_positions2)
        algo_positions.plot(ax=ax, label='algo position')
        algo_positions2.plot(ax=ax, label='algo position2', ls='--')
        # ax.set_title(aid)
        if i == len(aids) - 1:
            ax.legend()
        ax.grid()

        ax = axss[2][i]
        for col in ['costs.quadratic_impact_cost_mualgo', 'costs.linear_impact_cost_mualgo', 'costs.fees_mualgo',
                    'costs.fixed_fees_mualgo']:
            name = col.replace('mualgo', 'algo')
            (index_to_dt(prepend_zero(subdf[col])) / 10 ** 6).cumsum().plot(ax=ax, label=name)
        if i == len(aids) - 1:
            ax.legend()
        ax.grid()

    f.tight_layout()
    plt.show()
    plt.clf()

    total_asa_position_algo = pd.concat(total_asa_positions_algo)
    total_asa_position_algo.index = total_asa_position_algo.index.rename('time')
    total_asa_position_algo = total_asa_position_algo.groupby('time').sum()

    mualgo_states = sim_results.state_df.loc[0]
    mualgo_positions = prepend(mualgo_states['position'], sim_results.initial_state.mualgo_position)
    mualgo_positions = index_to_dt(mualgo_positions)
    algo_positions = mualgo_positions / 10 ** 6

    plt.plot(total_asa_position_algo, label='Total ASA position')
    plt.plot(algo_positions, label='Total Algo position')
    plt.plot(algo_positions + total_asa_position_algo, label='Total wealth')
    plt.legend()
    plt.grid()
    plt.show()
    plt.clf()


class TestReports(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_reports(self):
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
