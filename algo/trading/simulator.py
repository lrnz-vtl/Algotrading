import datetime
import unittest
import logging
from algo.trading.impact import ASAImpactState, AlgoPoolSwap, PositionAndImpactState, GlobalPositionAndImpactState
from algo.trading.optimizer import Optimizer
from algo.trading.costs import TradeCostsOther, TradeCostsMualgo
from matplotlib import pyplot as plt
import numpy as np
from algo.trading.signalprovider import PriceSignalProvider, DummySignalProvider
from algo.blockchain.stream import PoolState, PriceUpdate, stream_from_price_df
from algo.universe.universe import SimpleUniverse
from typing import Callable, Generator, Any, Optional
from dataclasses import dataclass
from datetime import timezone
from algo.blockchain.utils import load_algo_pools, make_filter_from_universe, int_to_tzaware_utc_datetime


@dataclass
class TradeRecord:
    time: datetime.datetime
    asset_buy_id: int
    asset_sell_id: int
    asset_buy_amount: int
    asset_sell_amount: int


@dataclass
class TradeInfo:
    trade: TradeRecord
    costs: TradeCostsMualgo
    asa_price: float


class Simulator:

    def __init__(self,
                 signal_providers: dict[int, PriceSignalProvider],
                 pos_impact_state: GlobalPositionAndImpactState,
                 universe: SimpleUniverse,
                 seed_time: datetime.timedelta,
                 price_stream: Generator[PriceUpdate, Any, Any],
                 simulation_step_seconds: int,
                 risk_coef: float,
                 log_trade: Callable[[TradeInfo], None]
                 ):

        self.log_trade = log_trade

        self.asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        self.optimizers = {asset_id: Optimizer(asset1=asset_id, risk_coef=risk_coef) for asset_id in self.asset_ids}

        self.logger = logging.getLogger(__name__)

        self.signal_providers = signal_providers
        self.pos_impact_state = pos_impact_state

        self.simulation_step = datetime.timedelta(seconds=simulation_step_seconds)

        self.price_stream = price_stream

        # The amount of time we spend seeding the prices and signals without trading
        self.seed_time = seed_time

        self.prices: dict[int, PoolState] = {}

    def trade_loop(self, time: datetime.datetime):

        for asset_id in self.asset_ids:
            self.logger.debug(f'Entering trade logic for asset {asset_id}')

            opt: Optimizer = self.optimizers[asset_id]

            signal_bps = self.signal_providers[asset_id].value(time)

            if asset_id not in self.prices:
                # self.logger.warning(f'Price for asset {asset_id} at time {time} not in data, skipping the trade logic.')
                continue

            current_asa_reserves = self.prices[asset_id].asset1_reserves
            current_mualgo_reserves = self.prices[asset_id].asset2_reserves

            opt_swap_quote = opt.fixed_sell_swap_quote(signal_bps=signal_bps,
                                                       pos_and_impact_state=self.pos_impact_state.asa_states[asset_id],
                                                       current_asa_reserves=current_asa_reserves,
                                                       current_mualgo_reserves=current_mualgo_reserves,
                                                       t=time,
                                                       current_mualgo_position=self.pos_impact_state.mualgo_position,
                                                       slippage=0)

            if opt_swap_quote is not None:
                # Pretend we always get the fill with zero slippage,

                if opt_swap_quote.amount_out.asset.id == 0:
                    out_reserves = current_mualgo_reserves
                    sell_position = self.pos_impact_state.asa_states[asset_id].asa_position
                elif opt_swap_quote.amount_out.asset.id > 0:
                    assert opt_swap_quote.amount_out.asset.id == asset_id
                    out_reserves = current_asa_reserves
                    sell_position = self.pos_impact_state.mualgo_position
                else:
                    raise ValueError

                assert opt_swap_quote.amount_out.amount <= out_reserves, \
                    f"Buy amount:{opt_swap_quote.amount_out.amount} > pool reserve {out_reserves}: " \
                    f"for asset {opt_swap_quote.amount_out.asset.id} in pool {asset_id} at time {time}"

                assert opt_swap_quote.amount_in.amount <= sell_position

                traded_swap = AlgoPoolSwap(
                    asset_buy=opt_swap_quote.amount_out.asset.id,
                    amount_buy=opt_swap_quote.amount_out.amount,
                    amount_sell=opt_swap_quote.amount_in.amount
                )

                self.pos_impact_state.update(
                    asset_id,
                    traded_swap,
                    current_mualgo_reserves,
                    current_asa_reserves,
                    time
                )

                if traded_swap.asset_buy == 0:
                    price_other = current_asa_reserves/current_mualgo_reserves
                else:
                    price_other = current_mualgo_reserves / current_asa_reserves

                asa_impact = self.pos_impact_state.asa_states[asset_id].impact.value(time)

                trade_costs = TradeCostsOther(buy_asset=traded_swap.asset_buy,
                                              buy_amount=traded_swap.amount_buy,
                                              buy_reserves=out_reserves,
                                              buy_asset_price_other=price_other,
                                              asa_impact=asa_impact).to_mualgo_basis()

                trade_record = TradeRecord(
                    time=time,
                    asset_buy_id=opt_swap_quote.amount_out.asset.id,
                    asset_sell_id=opt_swap_quote.amount_in.asset.id,
                    asset_buy_amount=opt_swap_quote.amount_out.amount,
                    asset_sell_amount=opt_swap_quote.amount_in.amount
                )

                trade_info = TradeInfo(trade_record, trade_costs, current_mualgo_reserves / current_asa_reserves)
                self.log_trade(trade_info)

    def update_state(self, time, asset_id, price_update):
        self.prices[asset_id] = price_update
        asa_price_mualgo = price_update.asset2_reserves / price_update.asset1_reserves
        self.signal_providers[asset_id].update(time, asa_price_mualgo)

    def run(self, end_time: datetime.datetime):

        # Takes values on the times where we run the trading loop
        current_time: Optional[datetime.datetime] = None

        initial_time: Optional[datetime.datetime] = None

        for x in self.price_stream:

            self.logger.debug(f'{x}')

            assert x.asset_ids[1] == 0
            asset_id, price_update = x.asset_ids[0], x.price_update

            # Time of the price update
            time = int_to_tzaware_utc_datetime(x.price_update.time)

            if initial_time is None:
                initial_time = time
                current_time = time

            assert time >= current_time

            while time - current_time > self.simulation_step:
                current_time = current_time + self.simulation_step
                # Trade only if we are not seeding
                if time - initial_time > self.seed_time:
                    self.logger.debug(f'Entering trading loop at sim time {current_time}')
                    self.trade_loop(current_time)
                else:
                    self.logger.debug(f'Still seeding at sim time {current_time}')

            # End the simulation
            if time > end_time:
                break

            self.update_state(time, asset_id, price_update)


class TestSimulator(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_simulator(self):
        risk_coef = 0.000000001
        price_cache_name = '20220209_prehack'
        universe_cache_name = 'liquid_algo_pools_nousd_prehack'
        impact_timescale_seconds = 5 * 60
        simulation_step_seconds = 5 * 60
        initial_mualgo_position = 1000000

        universe = SimpleUniverse.from_cache(universe_cache_name)

        filter_pair = make_filter_from_universe(universe)
        dfp = load_algo_pools(price_cache_name, 'prices', filter_pair)

        # Just choose some starting positions
        initial_positions = dfp.groupby('asset1')['asset1_reserves'].mean() // 1000

        seed_time = datetime.timedelta(days=1)

        initial_time = datetime.datetime(year=2021, month=11, day=10, tzinfo=timezone.utc)
        price_stream = stream_from_price_df(dfp, initial_time)

        end_time = datetime.datetime(year=2021, month=11, day=20, tzinfo=timezone.utc)

        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)

        pos_impact_states = {
            asset_id: PositionAndImpactState(ASAImpactState(impact_timescale_seconds),
                                             initial_positions.loc[asset_id])
            for asset_id in asset_ids
        }
        pos_impact_state = GlobalPositionAndImpactState(pos_impact_states, initial_mualgo_position)

        signal_providers = {
            asset_id: DummySignalProvider() for asset_id in asset_ids
        }

        def log_trade(x):
            self.logger.info(x)

        simulator = Simulator(universe=universe,
                              pos_impact_state=pos_impact_state,
                              signal_providers=signal_providers,
                              simulation_step_seconds=simulation_step_seconds,
                              risk_coef=risk_coef,
                              log_trade=log_trade,
                              seed_time=seed_time,
                              price_stream=price_stream
                              )

        simulator.run(end_time)

        # asa_reserves = 76845765066350
        # mualgo_reserves = 1517298655748
        #
        # price = mualgo_reserves / asa_reserves
        #
        # mualgo_position = 0
        # asa_position = asa_reserves // 1000
        # initial_asa_position_algo = price * asa_position / 10 ** 6
        #
        # self.logger.info(f'Initial ASA position in Algo units: {initial_asa_position_algo}')
        #
        # simulation_step_seconds = 5 * 60
        # simulation_step = datetime.timedelta(seconds=simulation_step_seconds)
        #
        # algo_reserves_decimal = mualgo_reserves / (10 ** 6)
        # self.logger.info(f'Decimal Pool algo reserves: {algo_reserves_decimal}')
        #
        # t = datetime.datetime.utcnow()
        # impact_state = ASAImpactState(decay_timescale_seconds=impact_timescale_seconds, t=t)
        #
        # # Simulate n days
        # n_days = 2
        #
        # steps = list(range((n_days * 24 * 60 * 60) // simulation_step_seconds))
        #
        # lost_fees = np.zeros(len(steps))
        # linear_impact_costs = np.zeros(len(steps))
        # quadratic_impact_costs = np.zeros(len(steps))
        #
        # impact_values = np.zeros(len(steps))
        # positions = np.zeros(len(steps))
        # algo_positions = np.zeros(len(steps))
        #
        # times = []
        #
        # for i in steps:
        #
        #     times.append(t)
        #
        #     impact_bps = impact_state.value(t)
        #     swap_info = optimiser.optimal_amount_swap(signal_bps=0,
        #                                               impact_bps=impact_bps,
        #                                               current_asa_position=asa_position,
        #                                               current_asa_reserves=asa_reserves,
        #                                               current_mualgo_reserves=mualgo_reserves
        #                                               )
        #     impact_values[i] = impact_state.value(t)
        #     positions[i] = asa_position * price / 10 ** 6
        #     algo_positions[i] = mualgo_position / 10 ** 6
        #
        #     if swap_info is not None:
        #         assert swap_info.optimal_swap.asset_buy == AssetType.ALGO
        #
        #         amount_buy = swap_info.optimal_swap.optimised_buy.amount
        #         amount_sell = int(swap_info.optimal_swap.optimised_buy.amount / price)
        #
        #         asa_position -= amount_sell
        #         mualgo_position += amount_buy
        #
        #         traded_swap = AlgoPoolSwap(asset_buy=0, amount_buy=amount_buy, amount_sell=amount_sell)
        #         impact_state.update(traded_swap, mualgo_reserves, asa_reserves, t)
        #
        #         lost_fees[i] = FIXED_FEE_ALGOS
        #         linear_impact_costs[i] = swap_info.trade_costs_mualgo.linear_impact_cost_mualgo / 10 ** 6
        #         quadratic_impact_costs[i] = swap_info.trade_costs_mualgo.quadratic_impact_cost_mualgo / 10 ** 6
        #
        #         self.logger.info(f'{t}: Sold ASA algo amount {traded_swap.amount_sell * price / 10 ** 6}.')
        #
        #     t += simulation_step
        #
        # self.logger.info(f'Final ASA position in Algo units: {asa_position * price / 10 ** 6}')
        # self.logger.info(f'Final Algo position: {mualgo_position / 10 ** 6}.')
        #
        # f, axs = plt.subplots(2, 2, figsize=(10, 12), sharex='col')
        #
        # axs[0][0].plot(times, positions, label='ASA position (algo basis)')
        # axs[0][0].plot(times, algo_positions, label='Algo position')
        # axs[0][0].legend()
        # axs[0][0].set_title('Positions')
        # axs[0][1].plot(times, impact_values)
        # axs[0][1].set_title('Impact')
        #
        # axs[1][0].plot(times, lost_fees.cumsum(), label='lost_fees')
        # axs[1][0].plot(times, linear_impact_costs.cumsum(), label='linear_impact_costs')
        # axs[1][0].plot(times, quadratic_impact_costs.cumsum(), label='quadratic_impact_costs')
        # axs[1][0].set_title('Accrued costs')
        #
        # axs[1][0].legend()
        # # f.tight_layout()
        # axs[1][0].tick_params(labelrotation=45)
        #
        # axs[1][1].tick_params(labelrotation=45)
        #
        # plt.show()
