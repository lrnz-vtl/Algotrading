from __future__ import annotations
import datetime
from dataclasses import dataclass
from algo.trading.costs import TradeCostsMualgo, REL_TOL
import math


@dataclass
class PriceInvariantTradeRecord:
    time: datetime.datetime
    asset_buy_id: int
    asset_sell_id: int
    asset_buy_amount: float
    asset_sell_amount: float

    def approx_eq_to(self, right: PriceInvariantTradeRecord):
        return self.time == right.time \
               and self.asset_buy_id == right.asset_buy_id \
               and self.asset_sell_id == right.asset_sell_id \
               and math.isclose(self.asset_sell_amount, right.asset_sell_amount, rel_tol=REL_TOL) \
               and math.isclose(self.asset_buy_amount, right.asset_buy_amount, rel_tol=REL_TOL)


@dataclass
class TradeRecord:
    time: datetime.datetime
    asset_buy_id: int
    asset_sell_id: int
    asset_buy_amount: int
    asset_sell_amount: int

    @staticmethod
    def zero(time: datetime.datetime, asset_buy_id, asset_sell_id) -> TradeRecord:
        return TradeRecord(time, asset_buy_id, asset_sell_id, 0, 0)

    def to_price_invariant(self, asa_price: float) -> PriceInvariantTradeRecord:
        if self.asset_buy_id > 0:
            asset_buy_amount = self.asset_buy_amount * asa_price
        else:
            asset_buy_amount = self.asset_buy_amount
        if self.asset_sell_id > 0:
            asset_sell_amount = self.asset_sell_amount * asa_price
        else:
            asset_sell_amount = self.asset_sell_amount
        return PriceInvariantTradeRecord(self.time, self.asset_buy_id, self.asset_sell_id, asset_buy_amount,
                                         asset_sell_amount)


@dataclass
class TradeInfo:
    trade: TradeRecord
    costs: TradeCostsMualgo
    asa_price: float

    def assert_price_covariant(self, right: TradeInfo):
        assert self.trade.to_price_invariant(asa_price=self.asa_price).approx_eq_to(right.trade.to_price_invariant(asa_price=right.asa_price))
        assert self.costs.approx_eq_to(right.costs)

    def price_covariant(self, right: TradeInfo) -> bool:
        try:
            self.assert_price_covariant(right)
        except AssertionError:
            return False
        return True
