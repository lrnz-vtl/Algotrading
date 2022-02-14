import datetime
import unittest
import logging
from .impact import ASAImpactState, AlgoPoolSwap
from .optimizer import Optimizer, AssetType, FIXED_FEE_ALGOS
from matplotlib import pyplot as plt
import numpy as np

class TestOptimizer(unittest.TestCase):

    def __init__(self, *args, **kwargs):

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_optimiser(self):
        risk_coef = 0.0000000001

        asa_asset_id = 470842789
        asa_reserves = 76845765066350
        mualgo_reserves = 1517298655748

        price = mualgo_reserves / asa_reserves

        mualgo_position = 0
        asa_position = asa_reserves // 1000
        initial_asa_position_algo = price * asa_position / 10 ** 6

        self.logger.info(f'Initial ASA position in Algo units: {initial_asa_position_algo}')

        impact_timescale_seconds = 5 * 60

        simulation_step_seconds = 5 * 60
        simulation_step = datetime.timedelta(seconds=simulation_step_seconds)

        algo_reserves_decimal = mualgo_reserves / (10 ** 6)
        self.logger.info(f'Decimal Pool algo reserves: {algo_reserves_decimal}')

        t = datetime.datetime.utcnow()
        impact_state = ASAImpactState(decay_timescale_seconds=impact_timescale_seconds, t=t)

        optimiser = Optimizer(asset1=asa_asset_id, risk_coef=risk_coef)

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
                linear_impact_costs[i] = swap_info.trade_costs_mualgo.linear_impact_cost_mulago / 10 ** 6
                quadratic_impact_costs[i] = swap_info.trade_costs_mualgo.quadratic_impact_cost_mulago / 10 ** 6

                self.logger.info(f'{t}: Sold ASA algo amount {traded_swap.amount_sell * price / 10 ** 6}.')

            t += simulation_step

        self.logger.info(f'Final Algo position: {mualgo_position / 10 ** 6}.')

        f, axs = plt.subplots(2, 2, figsize=(10, 12), sharex='col')

        axs[0][0].plot(times, positions, label='ASA position (algo basis)')
        axs[0][0].plot(times, algo_positions, label='Algo position')
        axs[0][0].legend()
        axs[0][0].set_title('Positions')
        axs[0][1].plot(times,impact_values)
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
