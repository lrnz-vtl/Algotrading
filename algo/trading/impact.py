import datetime
from dataclasses import dataclass
import numpy as np
from algo.trading.swapper import AlgoPoolSwap
import pandas as pd

# Leading order Taylor expansions of the functions below
impact_deflection_bps_perfraction = 2.0
avg_impact_deflection_bps_perfraction = 1.0


def impact_deflection_bps(asset_pool_percentage: float) -> float:
    """
    asset_pool_percentage: percentage of the token we take out relative to pool assets
    returns -> instantaneous percentage change of price of the token we buy in units of the token we sell
    """
    assert 0 <= asset_pool_percentage <= 1
    return 1.0 / (1.0 - asset_pool_percentage) ** 2 - 1.0


def avg_impact_deflection_bps(asset_pool_percentage: float) -> float:
    """
    asset_pool_percentage: percentage of the token we take out relative to pool assets
    returns -> The average percentage price deflection paid per a single transaction per token bought
    """
    assert 0 <= asset_pool_percentage <= 1
    return 1.0 / (1.0 - asset_pool_percentage) - 1.0


class ASAImpactState:
    def __init__(self, decay_timescale_seconds: int):
        self.state = 0
        # This does not really matter if s
        self.t = np.datetime64('NaT')
        self.decay_timescale_seconds = decay_timescale_seconds

    def update(self, swap: AlgoPoolSwap, mualgo_reserves: int, asa_reserves: int, t: datetime.datetime):

        if not pd.isnull(self.t):
            delta = t - self.t
            state = self.state * np.exp(- delta.total_seconds() / self.decay_timescale_seconds)
        else:
            state = 0

        if swap.asset_buy == 0:
            assert 0 <= swap.amount_buy <= mualgo_reserves
            algo_price_deflection = impact_deflection_bps(swap.amount_buy / mualgo_reserves)
            state += 1 / (1 + algo_price_deflection) - 1.0
        elif swap.asset_buy > 0:
            assert 0 <= swap.amount_buy <= asa_reserves
            asa_price_deflection = impact_deflection_bps(swap.amount_buy / mualgo_reserves)
            state += asa_price_deflection
        else:
            raise ValueError

        self.t = t
        self.state = state

    def value(self, t: datetime.datetime):
        if pd.isnull(self.t):
            return 0
        delta = t - self.t
        return self.state * np.exp(- delta.total_seconds() / self.decay_timescale_seconds)


@dataclass
class PositionAndImpactState:
    impact: ASAImpactState
    asa_position: int

    def update(self, traded_swap: AlgoPoolSwap, mualgo_reserves: int, asa_reserves: int, t: datetime.datetime):
        self.impact.update(traded_swap, mualgo_reserves, asa_reserves, t)
        if traded_swap.asset_buy == 0:
            self.asa_position -= traded_swap.amount_sell
            # We can't short
            assert self.asa_position >= 0
        else:
            self.asa_position += traded_swap.amount_buy


@dataclass
class GlobalPositionAndImpactState:
    asa_states: dict[int, PositionAndImpactState]
    mualgo_position: int

    def update(self, asa_id:int, traded_swap: AlgoPoolSwap,
               mualgo_reserves: int,
               asa_reserves: int,
               t: datetime.datetime):

        if traded_swap.asset_buy == 0:
            self.mualgo_position += traded_swap.amount_buy
        elif traded_swap.asset_buy > 0:
            self.mualgo_position -= traded_swap.amount_sell

        self.asa_states[asa_id].update(traded_swap, mualgo_reserves, asa_reserves, t)
