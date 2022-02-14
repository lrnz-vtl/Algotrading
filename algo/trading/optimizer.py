import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# TODO Check me
FIXED_FEE_ALGOS = 0.003
FIXED_FEE_MUALGOS = FIXED_FEE_ALGOS * 10 ** 6

FEE_BPS = (1000 / 997 - 1.0)

# TODO Measure me
EXPECTED_SLIPPAGE_BPS = 0.0


@dataclass
class OptimizedBuy:
    amount: int
    max_profitable_slippage_bps: float


@dataclass
class TradeCostsMualgo:
    quadratic_impact_cost_mulago: float
    linear_impact_cost_mulago: float


@dataclass
class TradeCostsOther:
    quadratic_impact_cost_other: float
    linear_impact_cost_other: float

    def to_mualgo_basis(self, asset_price: float) -> TradeCostsMualgo:
        return TradeCostsMualgo(self.quadratic_impact_cost_other * asset_price,
                                self.linear_impact_cost_other * asset_price
                                )


@dataclass
class OptimizedBuyInfo:
    optimized_buy: OptimizedBuy
    trade_costs_other: TradeCostsOther


class AssetType(Enum):
    ALGO = 1
    OTHER = 2


@dataclass
class OptimalSwap:
    asset_buy: AssetType
    optimised_buy: OptimizedBuy


@dataclass
class OptimalSwapInfo:
    optimal_swap: OptimalSwap
    trade_costs_mualgo: TradeCostsMualgo


def optimal_amount_buy_asset(signal_bps: float,
                             impact_bps: float,
                             current_asset_reserves: int,
                             current_other_asset_reserves: int,
                             quadratic_risk_penalty: float,
                             linear_risk_penalty: float,
                             fixed_fee_other: float) -> Optional[OptimizedBuyInfo]:
    # Do not let impact trade us more
    if impact_bps < 0:
        impact_bps = 0

    if impact_bps > 0:
        pass

    asset_price = current_other_asset_reserves / current_asset_reserves

    f_bps = signal_bps - impact_bps - FEE_BPS - EXPECTED_SLIPPAGE_BPS - linear_risk_penalty

    if f_bps < 0:
        return None

    # TODO Double check that fees multiply this too
    avg_impact_cost_coef = (1 + FEE_BPS) / current_asset_reserves

    # Expected profit in the reference currency
    # <Profit> = Amount * Price (f_bps - avg_impact_cost_coef * Amount) - quadratic_risk_penalty * Amount^2 - fixed_fee_other

    # TODO FIXME
    amount_profit_argmax = int(f_bps / (2 * (avg_impact_cost_coef + asset_price*quadratic_risk_penalty)))

    max_profit_other = (amount_profit_argmax * asset_price * f_bps / 2.0) - fixed_fee_other
    if max_profit_other <= 0:
        return None

    quadratic_impact_cost_other = (amount_profit_argmax ** 2) * asset_price * avg_impact_cost_coef
    linear_impact_cost_other = amount_profit_argmax * asset_price * impact_bps

    max_additional_slippage = f_bps - 2 * fixed_fee_other / (amount_profit_argmax * asset_price)
    return OptimizedBuyInfo(
        optimized_buy=OptimizedBuy(
            amount=amount_profit_argmax,
            max_profitable_slippage_bps=max_additional_slippage),
        trade_costs_other=TradeCostsOther(
            quadratic_impact_cost_other=quadratic_impact_cost_other,
            linear_impact_cost_other=linear_impact_cost_other
        )
    )


class Optimizer:
    def __init__(self, asset1: int, risk_coef: float):
        self.asset1 = asset1
        self.risk_coef = risk_coef
        assert self.asset1 > 0

        self.logger = logging.getLogger(__name__)

    def optimal_amount_swap(self, signal_bps: float,
                            impact_bps: float,
                            current_asa_position: int,
                            current_asa_reserves: int,
                            current_mualgo_reserves: int
                            ) -> Optional[OptimalSwapInfo]:

        asa_price_mualgo = current_mualgo_reserves / current_asa_reserves

        quadratic_risk_penalty = self.risk_coef * asa_price_mualgo**2
        linear_risk_penalty = 2.0 * self.risk_coef * current_asa_position * asa_price_mualgo**2

        optimized_asa_buy_info = optimal_amount_buy_asset(signal_bps=signal_bps,
                                                          impact_bps=impact_bps,
                                                          current_asset_reserves=current_asa_reserves,
                                                          current_other_asset_reserves=current_mualgo_reserves,
                                                          quadratic_risk_penalty = quadratic_risk_penalty,
                                                          linear_risk_penalty=linear_risk_penalty,
                                                          # Upper bound pessimistic estimate for the fixed cost: if we buy now we have to exit later, so pay it twice
                                                          fixed_fee_other=FIXED_FEE_MUALGOS * 2
                                                          )

        quadratic_risk_penalty = self.risk_coef
        linear_risk_penalty = - 2.0 * self.risk_coef * current_asa_position * asa_price_mualgo

        optimized_algo_buy_info = optimal_amount_buy_asset(signal_bps=1 / (1 + signal_bps) - 1.0,
                                                           impact_bps=1 / (1 + impact_bps) - 1.0,
                                                           current_asset_reserves=current_mualgo_reserves,
                                                           current_other_asset_reserves=current_asa_reserves,
                                                           quadratic_risk_penalty=quadratic_risk_penalty,
                                                           linear_risk_penalty=linear_risk_penalty,
                                                           fixed_fee_other=FIXED_FEE_MUALGOS / asa_price_mualgo
                                                           )

        assert optimized_asa_buy_info is None or optimized_algo_buy_info is None

        if optimized_asa_buy_info is not None:
            return OptimalSwapInfo(OptimalSwap(AssetType.OTHER, optimized_asa_buy_info.optimized_buy),
                                   optimized_asa_buy_info.trade_costs_other.to_mualgo_basis(1.0))
        elif optimized_algo_buy_info is not None:
            return OptimalSwapInfo(OptimalSwap(AssetType.ALGO, optimized_algo_buy_info.optimized_buy),
                                   optimized_algo_buy_info.trade_costs_other.to_mualgo_basis(asa_price_mualgo))
        else:
            return None
