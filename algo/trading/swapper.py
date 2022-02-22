from tinyman.v1.client import TinymanClient
import logging
from typing import Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass
from tinyman.v1.pools import SwapQuote
from algo.trading.costs import TradeCostsOther, TradeCostsMualgo
from algo.trading.trades import TradeInfo, TradeRecord
import datetime
from tinyman.v1.optin import prepare_asset_optin_transactions

# Max amount of Algo left locked in a pool
MAX_VALUE_LOCKED_ALGOS = 1


def lag_ms(dt: datetime.timedelta):
    return int(dt.total_seconds() * 1000)


@dataclass
class AlgoPoolSwap:
    asset_buy: int
    amount_buy: int
    amount_sell: int
    amount_buy_with_slippage: int
    amount_sell_with_slippage: int
    txid: str

    def make_costs(self, current_asa_reserves: int, current_mualgo_reserves: int,
                   impact_before_trade: float) -> TradeCostsMualgo:
        if self.asset_buy == 0:
            price_other = current_asa_reserves / current_mualgo_reserves
        else:
            price_other = current_mualgo_reserves / current_asa_reserves

        if self.asset_buy == 0:
            out_reserves = current_mualgo_reserves
        elif self.asset_buy > 0:
            out_reserves = current_asa_reserves
        else:
            raise ValueError

        return TradeCostsOther(buy_asset=self.asset_buy,
                               buy_amount=self.amount_buy,
                               buy_reserves=out_reserves,
                               buy_asset_price_other=price_other,
                               asa_impact=impact_before_trade).to_mualgo_basis()

    def make_record(self, time: datetime.datetime, asa_id: int):
        if asa_id == self.asset_buy:
            asset_sell_id = 0
        else:
            assert self.asset_buy == 0
            asset_sell_id = asa_id

        return TradeRecord(
            time=time,
            asset_buy_id=self.asset_buy,
            asset_sell_id=asset_sell_id,
            asset_buy_amount=self.amount_buy,
            asset_sell_amount=self.amount_sell,
            asset_buy_amount_with_slippage=self.amount_buy_with_slippage,
            asset_sell_amount_with_slippage=self.amount_sell_with_slippage,
            txid=self.txid
        )


@dataclass
class MaybeTradedSwap:
    swap: Optional[AlgoPoolSwap]
    time: datetime.datetime


@dataclass
class TimedSwapQuote:
    time: datetime.datetime
    quote: SwapQuote
    mualgo_reserves_at_opt: int
    asa_reserves_at_opt: int


@dataclass
class RedeemedAmounts:
    asa_amount: int
    mualgo_amount: int


class Swapper(ABC):
    @abstractmethod
    def attempt_transaction(self, quote: TimedSwapQuote) -> MaybeTradedSwap:
        pass

    @abstractmethod
    def fetch_excess_amounts(self, asa_price: float) -> RedeemedAmounts:
        pass


class ProductionSwapper(Swapper):
    def __init__(self, aid: int, client: TinymanClient, address: str, key: str, refresh_prices: bool):
        self.pool = client.fetch_pool(aid, 0)
        self.address = address
        self.aid = aid
        self.key = key
        self.logger = logging.getLogger(__name__)
        self.client = client
        assert self.pool.exists
        self._asset_optin()
        self.refresh_prices = refresh_prices

    def _client_optin(self):
        if not self.client.is_opted_in():
            self.logger.info('Account not opted into app, opting in now..')
            transaction_group = self.client.prepare_app_optin_transactions()
            transaction_group.sign_with_private_key(self.address, self.key)
            res = self.client.submit(transaction_group, wait=True)
            self.logger.info(f'Opted into app, {res}')

    def _asset_optin(self):
        acc_info = self.client.algod.account_info(self.address)
        for a in acc_info['assets']:
            if a['asset-id'] == self.aid:
                return

        self.logger.info(f'Account not opted into asset {self.aid}, opting in now..')

        txn_group = prepare_asset_optin_transactions(
            asset_id=self.aid,
            sender=self.address,
            suggested_params=self.client.algod.suggested_params()
        )
        txn_group.sign_with_private_key(self.address, self.key)
        res = self.client.submit(txn_group, wait=True)
        self.logger.info(f'Opted into asset, {res}')

    def attempt_transaction(self, quote: TimedSwapQuote) -> MaybeTradedSwap:

        if self.refresh_prices:
            self.pool.refresh()
            if self.pool.asset1_reserves != quote.asa_reserves_at_opt or self.pool.asset2_reserves != quote.mualgo_reserves_at_opt:
                self.logger.warning(f'Refreshed (ASA, Algo) reserves are different from those at optimisation'
                                    f'\n ({self.pool.asset1_reserves, self.pool.asset2_reserves}) != '
                                    f'({quote.asa_reserves_at_opt, quote.mualgo_reserves_at_opt})')

        transaction_group = self.pool.prepare_swap_transactions_from_quote(quote.quote)
        transaction_group.sign_with_private_key(self.address, self.key)
        res = self.client.submit(transaction_group)

        time = datetime.datetime.utcnow()

        return MaybeTradedSwap(
            AlgoPoolSwap(
                asset_buy=quote.quote.amount_out.asset.id,
                amount_buy=quote.quote.amount_out.amount,
                amount_sell=quote.quote.amount_in.amount,
                amount_buy_with_slippage=quote.quote.amount_out_with_slippage.amount,
                amount_sell_with_slippage=quote.quote.amount_in_with_slippage.amount,
                txid=res['txid']
            ),
            time=time
        )

    def fetch_excess_amounts(self, asa_price: float) -> RedeemedAmounts:

        time = datetime.datetime.utcnow()
        self.logger.debug(f'Entering fetch_excess_amounts for asset {self.aid}')

        excess = self.pool.fetch_excess_amounts(self.address)

        ret = RedeemedAmounts(0, 0)

        for asset, asset_amount in excess.items():

            amount = asset_amount.amount

            if asset.id == 0:
                algo_value = amount / (10 ** 6)
                ret.mualgo_amount = amount
            elif asset.id == self.aid:
                algo_value = amount * asa_price / (10 ** 6)
                ret.asa_amount = amount
            else:
                raise ValueError

            if algo_value > MAX_VALUE_LOCKED_ALGOS:
                transaction_group = self.pool.prepare_redeem_transactions(amount)
                transaction_group.sign_with_private_key(self.address, self.key)
                result = self.client.submit(transaction_group, wait=True)
                if result['pool_error'] != '':
                    self.logger.error(f"Redemption may have failed with errror {result['pool_error']}")
                self.logger.info(f'Redeemed {asset_amount} from pool {self.aid}, result={result}')

                if asset.id == 0:
                    ret.mualgo_amount = amount
                else:
                    ret.asa_amount = amount

        dt = lag_ms(datetime.datetime.utcnow() - time)
        self.logger.debug(f'Spent {dt} ms inside fetch_excess_amounts')

        return ret


class SimulationSwapper(Swapper):

    def fetch_excess_amounts(self, asa_price: float):
        pass

    def attempt_transaction(self, quote: TimedSwapQuote) -> MaybeTradedSwap:
        return MaybeTradedSwap(
            AlgoPoolSwap(
                asset_buy=quote.quote.amount_out.asset.id,
                amount_buy=quote.quote.amount_out.amount,
                amount_sell=quote.quote.amount_in.amount,
                amount_buy_with_slippage=quote.quote.amount_out_with_slippage.amount,
                amount_sell_with_slippage=quote.quote.amount_in_with_slippage.amount,
                txid=""
            ),
            time=quote.time
        )
