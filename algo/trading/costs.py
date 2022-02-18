from algo.trading.impact import ASAImpactState
from dataclasses import dataclass

# TODO Check me
_FIXED_FEE_ALGOS = 0.003
FIXED_FEE_MUALGOS = int(_FIXED_FEE_ALGOS * 10 ** 6)

FEE_BPS = (1000 / 997 - 1.0)

# TODO Measure me
EXPECTED_SLIPPAGE_BPS = 0.0


def reserves_to_avg_impact_cost_coef(reserves: int):
    # TODO Double check that fees multiply this too
    return (1 + FEE_BPS) / reserves


def buy_quad_impact_cost_other(amount: int, price: float, reserves: int) -> float:
    return amount * amount * price * reserves_to_avg_impact_cost_coef(reserves)


def buy_lin_impact_cost_other(amount: int, price: float, impact_bps: float) -> float:
    if impact_bps < 0:
        return 0
    return amount * price * impact_bps


@dataclass
class TradeCostsMualgo:
    quadratic_impact_cost_mualgo: float
    linear_impact_cost_mualgo: float
    fees_mualgo: float
    fixed_fees_mualgo: float


class TradeCostsOther:
    quadratic_impact_cost_other: float
    linear_impact_cost_other: float
    fees_other: float
    fixed_fees_other: float

    def __init__(self, buy_asset: int,
                 buy_amount: int,
                 buy_reserves: int,
                 buy_asset_price_other: float,
                 asa_impact: float
                 ):
        self._buy_asset = buy_asset
        self._buy_asset_price_other = buy_asset_price_other

        self.quadratic_impact_cost_other = buy_quad_impact_cost_other(amount=buy_amount,
                                                                      price=buy_asset_price_other,
                                                                      reserves=buy_reserves
                                                                      )
        if buy_asset == 0:
            impact_bps = 1 / (1 + asa_impact) - 1.0
        else:
            impact_bps = asa_impact
        self.linear_impact_cost_other = buy_lin_impact_cost_other(
            amount=buy_amount,
            price=buy_asset_price_other,
            impact_bps=impact_bps
        )
        self.fees_other = FEE_BPS * buy_amount * buy_asset_price_other
        if buy_asset > 0:
            self.fixed_fees_other = FIXED_FEE_MUALGOS
        else:
            self.fixed_fees_other = FIXED_FEE_MUALGOS / buy_asset_price_other

    def __post_init__(self):
        assert self.linear_impact_cost_other > 0

    def to_mualgo_basis(self) -> TradeCostsMualgo:
        if self._buy_asset == 0:
            price = 1.0
        else:
            price = self._buy_asset_price_other

        return TradeCostsMualgo(
            quadratic_impact_cost_mualgo=self.quadratic_impact_cost_other * price,
            linear_impact_cost_mualgo=self.linear_impact_cost_other * price,
            fees_mualgo=self.fees_other * price,
            fixed_fees_mualgo=FIXED_FEE_MUALGOS
        )
