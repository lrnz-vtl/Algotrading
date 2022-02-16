import datetime
import unittest
import logging
from .impact import ASAImpactState, AlgoPoolSwap
from .optimizer import Optimizer, AssetType, FIXED_FEE_ALGOS
from matplotlib import pyplot as plt
import numpy as np
from algo.trading.signalprovider import PriceSignalProvider, DummySignalProvider
from algo.blockchain.stream import PriceVolumeStream, DataStream, only_price, filter_last_prices, PoolState
from abc import ABC, abstractmethod
from algo.universe.universe import SimpleUniverse
from algo.blockchain.algo_requests import QueryParams
from typing import Callable
from dataclasses import dataclass
from datetime import timezone





@dataclass
class TradeInfo:
    pass


class Simulator:

    def __init__(self,
                 signal_providers: dict[int, PriceSignalProvider],
                 pos_impact_states: dict[int, PositionAndImpactState],
                 universe: SimpleUniverse,
                 start_date: datetime.date,
                 simulation_step_seconds: int,
                 risk_coef: float,
                 mualgo_reserves: float,
                 log_trade: Callable[[TradeInfo], None]
                 ):

        self.log_trade = log_trade

        self.asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        self.optimizers = {asset_id: Optimizer(asset1=asset_id, risk_coef=risk_coef) for asset_id in self.asset_ids}

        self.logger = logging.getLogger(__name__)

        self.signal_providers = signal_providers
        self.pos_impact_states = pos_impact_states

        self.simulation_step = datetime.timedelta(seconds=simulation_step_seconds)

        data_stream = DataStream(universe, query_params=QueryParams(after_time=start_date))
        self.price_volume_stream = PriceVolumeStream(data_stream)

        self.initial_time = datetime.datetime(year=start_date.year, month=start_date.month, day=start_date.day, tzinfo=timezone.utc)

        assert isinstance(self.initial_time, datetime.datetime)
        assert self.initial_time == timezone.utc

        self.mualgo_reserves = mualgo_reserves

        self.prices: dict[int, PoolState] = {}

    def trade_loop(self, time: datetime.datetime):

        self.logger.debug(f'Entering trading loop at time {time}')

        for asset_id in self.asset_ids:
            opt: Optimizer = self.optimizers[asset_id]

            signal_bps = self.signal_providers[asset_id].value(time)
            impact_bps = self.pos_impact_states[asset_id].impact.value(time)
            current_asa_position = self.pos_impact_states[asset_id].asa_position

            current_asa_reserves = self.prices[asset_id].asset1_reserves
            current_mualgo_reserves = self.prices[asset_id].asset2_reserves

            opt_swap = opt.optimal_amount_swap(signal_bps,
                                               impact_bps,
                                               current_asa_position,
                                               current_asa_reserves,
                                               current_mualgo_reserves
                                               )
            if opt_swap:
                amount_buy = opt_swap.optimal_swap.optimised_buy.amount

    def run(self):
        current_time = self.initial_time

        gen = only_price(
            filter_last_prices(
                self.price_volume_stream.scrape()
            )
        )

        for x in gen:
            assert x.asset_ids[1] == 0
            asset_id, price_update = x.asset_ids[0], x.price_update

            time = datetime.datetime.utcfromtimestamp(x.price_update.time)
            assert time >= current_time

            while time - current_time > self.simulation_step:
                current_time = current_time + self.simulation_step
                self.trade_loop(current_time)

            for asset_id in self.asset_ids:
                self.prices[asset_id] = price_update
                asa_price_mualgo = price_update.asset2_reserves / price_update.asset1_reserves
                self.signal_providers[asset_id].update(time, asa_price_mualgo)




class TestSimulator(unittest.TestCase):

    def __init__(self, *args, **kwargs):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_simulator(self):

        risk_coef = 0.0000000001
        universe_cache_name = 'liquid_algo_pools_nousd_prehack'
        impact_timescale_seconds = 5 * 60
        simulation_step_seconds = 5 * 60
        mualgo_reserves = 0

        universe = SimpleUniverse.from_cache(universe_cache_name)

        initial_time = datetime.datetime(year=2021, month=11, day=1, tzinfo=timezone.utc)
        end_time = datetime.datetime(year=2021, month=11, day=3, tzinfo=timezone.utc).date()

        initial_date = initial_time.date()

        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        pos_impact_states = {
            asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds, initial_time), 0)
            for asset_id in asset_ids
        }

        signal_providers = {
            asset_id: DummySignalProvider() for asset_id in asset_ids
        }

        simulator = Simulator(universe=universe,
                              pos_impact_states=pos_impact_states,
                              signal_providers=signal_providers,
                              simulation_step_seconds=simulation_step_seconds,
                              mualgo_reserves=mualgo_reserves,
                              risk_coef=risk_coef,
                              start_date=initial_date
                              )

        asa_reserves = 76845765066350
        mualgo_reserves = 1517298655748

        price = mualgo_reserves / asa_reserves

        mualgo_position = 0
        asa_position = asa_reserves // 1000
        initial_asa_position_algo = price * asa_position / 10 ** 6

        self.logger.info(f'Initial ASA position in Algo units: {initial_asa_position_algo}')

        simulation_step_seconds = 5 * 60
        simulation_step = datetime.timedelta(seconds=simulation_step_seconds)

        algo_reserves_decimal = mualgo_reserves / (10 ** 6)
        self.logger.info(f'Decimal Pool algo reserves: {algo_reserves_decimal}')

        t = datetime.datetime.utcnow()
        impact_state = ASAImpactState(decay_timescale_seconds=impact_timescale_seconds, t=t)

        # Simulate n days
        n_days = 2

        steps = list(range((n_days * 24 * 60 * 60) // simulation_step_seconds))

        lost_fees = np.zeros(len(steps))
        linear_impact_costs = np.zeros(len(steps))
        quadratic_impact_costs = np.zeros(len(steps))

        impact_values = np.zeros(len(steps))
        positions = np.zeros(len(steps))
        algo_positions = np.zeros(len(steps))

        times = []

        for i in steps:

            times.append(t)

            impact_bps = impact_state.value(t)
            swap_info = optimiser.optimal_amount_swap(signal_bps=0,
                                                      impact_bps=impact_bps,
                                                      current_asa_position=asa_position,
                                                      current_asa_reserves=asa_reserves,
                                                      current_mualgo_reserves=mualgo_reserves
                                                      )
            impact_values[i] = impact_state.value(t)
            positions[i] = asa_position * price / 10 ** 6
            algo_positions[i] = mualgo_position / 10 ** 6

            if swap_info is not None:
                assert swap_info.optimal_swap.asset_buy == AssetType.ALGO

                amount_buy = swap_info.optimal_swap.optimised_buy.amount
                amount_sell = int(swap_info.optimal_swap.optimised_buy.amount / price)

                asa_position -= amount_sell
                mualgo_position += amount_buy

                traded_swap = AlgoPoolSwap(asset_buy=0, amount_buy=amount_buy, amount_sell=amount_sell)
                impact_state.update(traded_swap, mualgo_reserves, asa_reserves, t)

                lost_fees[i] = FIXED_FEE_ALGOS
                linear_impact_costs[i] = swap_info.trade_costs_mualgo.linear_impact_cost_mualgo / 10 ** 6
                quadratic_impact_costs[i] = swap_info.trade_costs_mualgo.quadratic_impact_cost_mualgo / 10 ** 6

                self.logger.info(f'{t}: Sold ASA algo amount {traded_swap.amount_sell * price / 10 ** 6}.')

            t += simulation_step

        self.logger.info(f'Final ASA position in Algo units: {asa_position * price / 10 ** 6}')
        self.logger.info(f'Final Algo position: {mualgo_position / 10 ** 6}.')

        f, axs = plt.subplots(2, 2, figsize=(10, 12), sharex='col')

        axs[0][0].plot(times, positions, label='ASA position (algo basis)')
        axs[0][0].plot(times, algo_positions, label='Algo position')
        axs[0][0].legend()
        axs[0][0].set_title('Positions')
        axs[0][1].plot(times, impact_values)
        axs[0][1].set_title('Impact')

        axs[1][0].plot(times, lost_fees.cumsum(), label='lost_fees')
        axs[1][0].plot(times, linear_impact_costs.cumsum(), label='linear_impact_costs')
        axs[1][0].plot(times, quadratic_impact_costs.cumsum(), label='quadratic_impact_costs')
        axs[1][0].set_title('Accrued costs')

        axs[1][0].legend()
        # f.tight_layout()
        axs[1][0].tick_params(labelrotation=45)

        axs[1][1].tick_params(labelrotation=45)

        plt.show()
