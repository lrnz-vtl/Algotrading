import datetime
import unittest
import logging
from .impact import ASAImpactState, AlgoPoolSwap
from .optimizer import Optimizer, OptimalSwap, OptimizedBuy, AssetType, FIXED_FEE_ALGOS
from matplotlib import pyplot as plt


class TestOptimizer(unittest.TestCase):

    def __init__(self, *args, **kwargs):

        logging.basicConfig(level=logging.ERROR)
        self.logger = logging.getLogger(__name__)

        super().__init__(*args, **kwargs)

    def test_optimiser(self):
        risk_bps_per_algo = 0.0001

        asa_asset_id = 470842789
        asa_reserves = 76845765066350
        mualgo_reserves = 1517298655748

        price = mualgo_reserves / asa_reserves

        algo_position = 0
        asa_position = asa_reserves // 1000
        initial_asa_position_algo = price * asa_position / 10 ** 6

        self.logger.info(f'Initial ASA position in Algo units: {initial_asa_position_algo}')

        impact_timescale_seconds = 60 * 60

        simulation_step_seconds = 5
        simulation_step = datetime.timedelta(seconds=simulation_step_seconds)

        algo_reserves_decimal = mualgo_reserves / (10 ** 6)
        self.logger.info(f'Decimal Pool algo reserves: {algo_reserves_decimal}')

        t = datetime.datetime.utcnow()
        impact_state = ASAImpactState(decay_timescale_seconds=impact_timescale_seconds, t=t)

        optimiser = Optimizer(asset1=asa_asset_id, risk_bps_per_algo=risk_bps_per_algo)

        impact_values = []
        positions = []

        n_days = 10

        lost_fees = 0

        # Simulate 5 days
        for i in range((n_days * 24 * 60 * 60) // simulation_step_seconds):

            impact_bps = impact_state.value(t)
            swap = optimiser.optimal_amount_swap(signal_bps=0,
                                                 impact_bps=impact_bps,
                                                 current_asa_position=asa_position,
                                                 current_asa_reserves=asa_reserves,
                                                 current_mualgo_reserves=mualgo_reserves
                                                 )
            if swap is not None:
                assert swap.asset_buy == AssetType.ALGO

                amount_buy = swap.optimised_buy.amount
                amount_sell = int(swap.optimised_buy.amount * price)

                asa_position -= amount_sell
                algo_position += amount_buy

                traded_swap = AlgoPoolSwap(asset_buy=0, amount_buy=amount_buy, amount_sell=amount_sell)
                impact_state.update(traded_swap, mualgo_reserves, asa_reserves, t)

                asa_position_algo = asa_position * price / 10 ** 6
                impact_state_value = impact_state.value(t)

                impact_values.append(impact_state_value)
                positions.append(asa_position_algo)
                lost_fees += FIXED_FEE_ALGOS

                self.logger.info(f'{t}: Sold ASA algo amount {traded_swap.amount_sell * price / 10 ** 6}. '
                                 f'ASA algo position = {asa_position_algo}, '
                                 f'Impact state = {impact_state_value}')

            t += simulation_step

        f, axs = plt.subplots(1, 2, figsize=(10, 5))
        axs[0].plot(positions)
        axs[0].set_title('Position')
        axs[1].plot(impact_values)
        axs[1].set_title('Impact')
        f.suptitle(f'Lost fees = {lost_fees}')
        plt.show()
