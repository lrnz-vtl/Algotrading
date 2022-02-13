import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

# TODO Check me
FIXED_FEE_ALGOS = 0.003

FEE_BPS = (1000 / 997 - 1.0)

# TODO Measure me
EXPECTED_SLIPPAGE_BPS = 0.0


@dataclass
class OptimizedBuy:
    amount: int
    max_profitable_slippage_bps: float


class AssetType(Enum):
    ALGO = 1
    OTHER = 2


@dataclass
class OptimalSwap:
    asset_buy: AssetType
    optimised_buy: OptimizedBuy


def optimal_amount_buy_asset(signal_bps: float,
                             impact_bps: float,
                             current_asset_reserves: int,
                             current_other_asset_reserves: int,
                             risk_bps: float,
                             fixed_fee_other: float) -> Optional[OptimizedBuy]:
    # Do not let impact trade us more
    if impact_bps < 0:
        impact_bps = 0

    asset_price = current_other_asset_reserves / current_asset_reserves

    f_bps = signal_bps - impact_bps - FEE_BPS - EXPECTED_SLIPPAGE_BPS + risk_bps

    if f_bps < 0:
        return None

    # TODO Double check that fees multiply this too
    avg_impact_cost_coef = (1 + FEE_BPS) / current_asset_reserves

    # Expected profit in the reference currency
    # <Profit> = Amount * Price (f_bps - avg_impact_cost_coef * Amount) - fixed_fee_other

    amount_profit_argmax = int(f_bps / (2 * avg_impact_cost_coef))

    max_profit_other = (amount_profit_argmax * asset_price * f_bps / 2.0) - fixed_fee_other
    if max_profit_other <= 0:
        return None

    max_additional_slippage = f_bps - 2 * fixed_fee_other / (amount_profit_argmax * asset_price)
    return OptimizedBuy(amount_profit_argmax, max_additional_slippage)


class Optimizer:
    def __init__(self, asset1: int, risk_bps_per_algo: float):
        self.asset1 = asset1
        self.risk_bps_per_mualgo = risk_bps_per_algo / 10**6
        assert self.asset1 > 0

        self.logger = logging.getLogger(__name__)

    def optimal_amount_swap(self, signal_bps: float,
                            impact_bps: float,
                            current_asa_position: int,
                            current_asa_reserves: int,
                            current_mualgo_reserves: int
                            ) -> Optional[OptimalSwap]:

        asa_price_mualgo = current_mualgo_reserves / current_asa_reserves

        optimized_asa_buy = optimal_amount_buy_asset(signal_bps=signal_bps,
                                                     impact_bps=impact_bps,
                                                     current_asset_reserves=current_asa_reserves,
                                                     current_other_asset_reserves=current_mualgo_reserves,
                                                     risk_bps=-current_asa_position * asa_price_mualgo * self.risk_bps_per_mualgo,
                                                     # Upper bound pessimistic estimate for the fixed cost: if we buy now we have to exit later, so pay it twice
                                                     fixed_fee_other=FIXED_FEE_ALGOS * 2
                                                     )

        optimized_algo_buy = optimal_amount_buy_asset(signal_bps=1 / (1 + signal_bps) - 1.0,
                                                      impact_bps=1 / (1 + impact_bps) - 1.0,
                                                      current_asset_reserves=current_mualgo_reserves,
                                                      current_other_asset_reserves=current_asa_reserves,
                                                      risk_bps=+current_asa_position * asa_price_mualgo * self.risk_bps_per_mualgo,
                                                      # TODO Check me
                                                      fixed_fee_other=FIXED_FEE_ALGOS
                                                      )

        assert optimized_asa_buy is None or optimized_asa_buy is None

        if optimized_asa_buy is not None:
            return OptimalSwap(AssetType.OTHER, optimized_asa_buy)
        elif optimized_algo_buy is not None:
            self.logger.debug(f'Selling ASA and buying {optimized_algo_buy.amount / 10**6} algos')
            return OptimalSwap(AssetType.ALGO, optimized_algo_buy)
        else:
            return None



