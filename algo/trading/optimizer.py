import datetime
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from algo.trading.impact import PositionAndImpactState, ASAImpactState
from algo.trading.costs import FIXED_FEE_MUALGOS, FEE_BPS, EXPECTED_SLIPPAGE_BPS, reserves_to_avg_impact_cost_coef
from tinyman.v1.pools import SwapQuote, AssetAmount, Asset
import numpy as np
import math
import warnings

# TODO Don't make me global
warnings.simplefilter("error", RuntimeWarning)

logger = logging.getLogger(__name__)


# The quadratic impact approximation is not accurate beyond this point
RESERVE_PERCENTAGE_CAP = 0.1

# Only allowed to go 10% above the previous cap after optimisation
RESERVE_PERCENTAGE_CAP_TOLERANCE = 0.1


@dataclass
class OptimizedBuy:
    # Optimized amount which maximizes profit
    amount: int
    # The min amount at which trade would be profitable (it's not zero because of the fixed fee)
    min_profitable_amount: int

    def __post_init__(self):
        assert self.amount > 0
        assert self.min_profitable_amount > 0
        assert self.amount >= self.min_profitable_amount


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
                             quadratic_risk_penalty: float,
                             linear_risk_penalty: float,
                             fixed_fee_other: float) -> Optional[OptimizedBuy]:
    # Do not let impact trade us more
    if impact_bps < 0:
        impact_bps = 0

    asset_price = current_other_asset_reserves / current_asset_reserves

    f_bps = signal_bps - impact_bps - FEE_BPS - EXPECTED_SLIPPAGE_BPS - linear_risk_penalty

    if f_bps < 0:
        return None

    avg_impact_cost_coef = reserves_to_avg_impact_cost_coef(current_asset_reserves)

    # Expected profit in the reference currency
    # <Profit> = Amount * Price (f_bps - avg_impact_cost_coef * Amount) - quadratic_risk_penalty * Amount^2 - fixed_fee_other

    lin = asset_price * f_bps
    quad = quadratic_risk_penalty + asset_price * avg_impact_cost_coef
    const = fixed_fee_other

    avg_impact_cost_coef_dbg = (asset_price * avg_impact_cost_coef) / (asset_price)
    quadratic_risk_penalty_dbg = quadratic_risk_penalty / (asset_price)
    quad_dbg = quad / (asset_price)

    # <Profit> = lin * Amount - quad * Amount^2 - const

    amount_profit_argmax = int(lin / (2 * quad))

    const_dbg = const / asset_price
    amount_dbg = amount_profit_argmax

    max_profit_other = lin ** 2 / (4 * quad) - const
    if max_profit_other <= 0:
        return None

    min_profitable_amount = (lin - np.sqrt(lin * lin - 4 * const * quad)) / (2 * quad)
    assert min_profitable_amount > 0
    min_profitable_amount = math.ceil(min_profitable_amount)

    capped_amount = int(min(RESERVE_PERCENTAGE_CAP*current_asset_reserves, amount_profit_argmax))
    if capped_amount < min_profitable_amount:
        return None

    return OptimizedBuy(
        amount=capped_amount,
        min_profitable_amount=min_profitable_amount
    )


def fetch_fixed_input_swap_quote(asset1: Asset, asset2: Asset,
                                 asset1_reserves: int, asset2_reserves: int,
                                 amount_in: AssetAmount,
                                 slippage: float
                                 ) -> SwapQuote:
    asset_in, asset_in_amount = amount_in.asset, amount_in.amount

    if asset_in == asset1:
        asset_out = asset2
        input_supply = asset1_reserves
        output_supply = asset2_reserves
    else:
        asset_out = asset1
        input_supply = asset2_reserves
        output_supply = asset1_reserves

    input_supply = int(input_supply)
    output_supply = int(output_supply)
    # k = input_supply * output_supply
    # ignoring fees, k must remain constant
    # (input_supply + asset_in) * (output_supply - amount_out) = k
    k = input_supply * output_supply
    assert (k >= 0)
    asset_in_amount_minus_fee = (asset_in_amount * 997) / 1000
    swap_fees = asset_in_amount - asset_in_amount_minus_fee
    asset_out_amount = output_supply - (k / (input_supply + asset_in_amount_minus_fee))

    amount_out = AssetAmount(asset_out, int(asset_out_amount))

    quote = SwapQuote(
        swap_type='fixed-input',
        amount_in=amount_in,
        amount_out=amount_out,
        swap_fees=AssetAmount(amount_in.asset, int(swap_fees)),
        slippage=slippage,
    )
    return quote


class Optimizer:
    def __init__(self, asset1: int, risk_coef: float):
        self.asset1 = asset1
        self.risk_coef = risk_coef
        assert self.asset1 > 0
        self.logger = logging.getLogger(__name__)

    def optimal_amount_swap(self, signal_bps: float,
                            pos_and_impact_state: PositionAndImpactState,
                            current_asa_reserves: int,
                            current_mualgo_reserves: int,
                            t: datetime.datetime
                            ) -> Optional[OptimalSwap]:

        impact_bps = pos_and_impact_state.impact.value(t)
        current_asa_position = pos_and_impact_state.asa_position

        asa_price_mualgo = current_mualgo_reserves / current_asa_reserves

        # TODO Ideally we should make this also a function of the liquidity, not just the dollar value:
        #  the more illiquid the asset is, the more risky is it to hold it
        # FIXME This is probably wrong
        quadratic_risk_penalty = self.risk_coef * asa_price_mualgo ** 2
        linear_risk_penalty = 2.0 * self.risk_coef * current_asa_position.value * asa_price_mualgo ** 2

        optimized_asa_buy = None
        # optimized_asa_buy = optimal_amount_buy_asset(signal_bps=signal_bps,
        #                                              impact_bps=impact_bps,
        #                                              current_asset_reserves=current_asa_reserves,
        #                                              current_other_asset_reserves=current_mualgo_reserves,
        #                                              quadratic_risk_penalty=quadratic_risk_penalty,
        #                                              linear_risk_penalty=linear_risk_penalty,
        #                                              # Upper bound pessimistic estimate for the fixed cost: if we buy now we have to exit later, so pay it twice
        #                                              fixed_fee_other=FIXED_FEE_MUALGOS * 2
        #                                              )

        # TODO Ideally we should make this also a function of the liquidity, not just the dollar value:
        #  the more illiquid the asset is, the more risky is it to hold it
        quadratic_risk_penalty = self.risk_coef / asa_price_mualgo
        linear_risk_penalty = - 2.0 * self.risk_coef * current_asa_position.value * asa_price_mualgo

        optimized_algo_buy = optimal_amount_buy_asset(signal_bps=1 / (1 + signal_bps) - 1.0,
                                                      impact_bps=1 / (1 + impact_bps) - 1.0,
                                                      current_asset_reserves=current_mualgo_reserves,
                                                      current_other_asset_reserves=current_asa_reserves,
                                                      quadratic_risk_penalty=quadratic_risk_penalty,
                                                      linear_risk_penalty=linear_risk_penalty,
                                                      fixed_fee_other=FIXED_FEE_MUALGOS / asa_price_mualgo
                                                      )

        assert optimized_asa_buy is None or optimized_algo_buy is None

        if optimized_asa_buy is not None:
            return OptimalSwap(AssetType.OTHER, optimized_asa_buy)
        elif optimized_algo_buy is not None:
            return OptimalSwap(AssetType.ALGO, optimized_algo_buy)
        else:
            return None

    def fixed_sell_swap_quote(self, signal_bps: float,
                              pos_and_impact_state: PositionAndImpactState,
                              current_asa_reserves: int,
                              current_mualgo_reserves: int,
                              t: datetime.datetime,
                              current_mualgo_position: int,
                              slippage: float) -> Optional[SwapQuote]:

        optimal_swap = self.optimal_amount_swap(signal_bps, pos_and_impact_state,
                                                current_asa_reserves,
                                                current_mualgo_reserves,
                                                t)

        if optimal_swap is not None:

            if optimal_swap.asset_buy == AssetType.ALGO:
                # What we sell
                asset_in = Asset(self.asset1)
                input_supply = current_asa_reserves
                output_supply = current_mualgo_reserves
                sell_amount_available = pos_and_impact_state.asa_position

            else:
                # What we sell
                asset_in = Asset(0)
                input_supply = current_mualgo_reserves
                output_supply = current_asa_reserves
                # FIXME What should we subtract here?
                sell_amount_available = current_mualgo_position - FIXED_FEE_MUALGOS

            # Convert from int64 to int to avoid overflow errors
            input_supply = int(input_supply)
            output_supply = int(output_supply)

            k = input_supply * output_supply
            assert k >= 0

            def asset_in_from_asset_out(asset_out_amount):
                calculated_amount_in_without_fee = (k / (output_supply - asset_out_amount)) - input_supply
                asset_in_amount = int(calculated_amount_in_without_fee * 1000 / 997)
                return asset_in_amount

            # What we buy
            optimal_asset_out_amount = optimal_swap.optimised_buy.amount
            optimal_asset_in_amount = asset_in_from_asset_out(optimal_asset_out_amount)

            minimal_asset_out_amount = optimal_swap.optimised_buy.min_profitable_amount
            minimal_asset_in_amount = asset_in_from_asset_out(minimal_asset_out_amount)

            if sell_amount_available.value <= minimal_asset_in_amount:
                return None

            asset_in_amount = min(optimal_asset_in_amount, sell_amount_available.value)

            assert asset_in_amount <= sell_amount_available.value

            if asset_in_amount > 0:
                quote = fetch_fixed_input_swap_quote(Asset(self.asset1), Asset(0),
                                                     current_asa_reserves, current_mualgo_reserves,
                                                     AssetAmount(asset_in, asset_in_amount), slippage)

                # Beyond this the quadratic approximation for the impact cost breaks down
                # (more than 1% error in the calculation)
                assert quote.amount_out.amount < 0.1 * output_supply, \
                    f"amount_out={quote.amount_out.amount} >= {0.1*output_supply} = 0.1*output_supply"
                return quote

        return None
