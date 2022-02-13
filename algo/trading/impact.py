import datetime
from dataclasses import dataclass
import numpy as np

# Leading order Taylor expansions of the functions below
impact_deflection_bps_perfraction = 2.0
avg_impact_deflection_bps_perfraction = 1.0


def impact_deflection_bps(asset_pool_percentage: float) -> float:
    """
    asset_pool_percentage: percentage of the token we take out relative to pool assets
    returns -> instantaneous percentage change of price of the token we buy in units of the token we sell
    """
    assert 0 <= asset_pool_percentage <= 1
    return 1.0/(1.0 - asset_pool_percentage)**2 - 1.0


def avg_impact_deflection_bps(asset_pool_percentage: float) -> float:
    """
    asset_pool_percentage: percentage of the token we take out relative to pool assets
    returns -> The average percentage price deflection paid per a single transaction per token bought
    """
    assert 0 <= asset_pool_percentage <= 1
    return 1.0/(1.0 - asset_pool_percentage) - 1.0


@dataclass
class AlgoPoolSwap:
    asset_buy: int
    amount_buy: int
    amount_sell: int


class ASAImpactState:
    def __init__(self, decay_timescale_seconds: int, t: datetime.datetime):
        self.state = 0
        self.t = t
        self.decay_timescale_seconds = decay_timescale_seconds

    def update(self, swap: AlgoPoolSwap, algo_reserves: int, asa_reserves: int, t: datetime.datetime):

        delta = t - self.t

        state = self.state * np.exp(- delta.total_seconds() / self.decay_timescale_seconds)

        if swap.asset_buy == 0:
            assert 0 <= swap.amount_buy <= algo_reserves
            algo_price_deflection = impact_deflection_bps(swap.amount_buy / algo_reserves)
            state += 1 / (1 + algo_price_deflection) - 1.0
        elif swap.asset_buy > 0:
            assert 0 <= swap.amount_buy <= asa_reserves
            asa_price_deflection = impact_deflection_bps(swap.amount_buy / algo_reserves)
            state += asa_price_deflection
        else:
            raise ValueError

        self.t = t
        self.state = state

    def value(self, t: datetime.datetime):
        delta = t - self.t
        return self.state * np.exp(- delta.total_seconds() / self.decay_timescale_seconds)

