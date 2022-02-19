from __future__ import annotations
import datetime
import logging
from algo.trading.trades import TradeInfo, TradeRecord
from algo.trading.impact import GlobalPositionAndImpactState
from algo.trading.optimizer import Optimizer
from algo.trading.costs import TradeCostsOther, TradeCostsMualgo
from algo.trading.signalprovider import PriceSignalProvider
from algo.blockchain.stream import PoolState, PriceUpdate
from algo.universe.universe import SimpleUniverse
from typing import Callable, Generator, Any, Optional
from algo.blockchain.utils import int_to_tzaware_utc_datetime
from algo.trading.swapper import SimulationSwapper


def validate_sim_swap(opt_swap_quote, current_mualgo_reserves, current_asa_reserves, pos_impact_state, asset_id, time):
    if opt_swap_quote.amount_out.asset.id == 0:
        out_reserves = current_mualgo_reserves
        sell_position = pos_impact_state.asa_states[asset_id].asa_position
    elif opt_swap_quote.amount_out.asset.id > 0:
        assert opt_swap_quote.amount_out.asset.id == asset_id
        out_reserves = current_asa_reserves
        sell_position = pos_impact_state.mualgo_position
    else:
        raise ValueError

    assert opt_swap_quote.amount_out.amount <= out_reserves, \
        f"Buy amount:{opt_swap_quote.amount_out.amount} > pool reserve {out_reserves}: " \
        f"for asset {opt_swap_quote.amount_out.asset.id} in pool {asset_id} at time {time}"

    assert opt_swap_quote.amount_in.amount <= sell_position.value


class Simulator:

    def __init__(self,
                 signal_providers: dict[int, PriceSignalProvider],
                 pos_impact_state: GlobalPositionAndImpactState,
                 universe: SimpleUniverse,
                 seed_time: datetime.timedelta,
                 price_stream: Generator[PriceUpdate, Any, Any],
                 simulation_step_seconds: int,
                 risk_coef: float,
                 log_null_trades: bool = False,
                 log_state: Optional[Callable[[GlobalPositionAndImpactState], None]] = None
                 ):

        self.log_state = log_state
        self.asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools)
        self.optimizers = {asset_id: Optimizer(asset1=asset_id, risk_coef=risk_coef) for asset_id in self.asset_ids}
        self.logger = logging.getLogger(__name__)
        self.signal_providers = signal_providers
        self.pos_impact_state = pos_impact_state
        self.simulation_step = datetime.timedelta(seconds=simulation_step_seconds)
        self.price_stream = price_stream
        # The amount of time we spend seeding the prices and signals without trading
        self.seed_time = seed_time
        self.prices: dict[int, PoolState] = {}
        self.swapper = {aid: SimulationSwapper() for aid in self.asset_ids}
        self.log_null_trades = log_null_trades

    def trade_loop(self, time: datetime.datetime, log_trade: Callable[[TradeInfo], None]):

        for asset_id in self.asset_ids:

            self.logger.debug(f'Entering trade logic for asset {asset_id}')
            opt: Optimizer = self.optimizers[asset_id]
            signal_bps = self.signal_providers[asset_id].value(time)

            if asset_id not in self.prices:
                self.logger.warning(f'Price for asset {asset_id} at time {time} not in data, skipping the trade logic.')
                continue

            current_asa_reserves = self.prices[asset_id].asset1_reserves
            current_mualgo_reserves = self.prices[asset_id].asset2_reserves
            opt_swap_quote = opt.fixed_sell_swap_quote(signal_bps=signal_bps,
                                                       pos_and_impact_state=self.pos_impact_state.asa_states[asset_id],
                                                       current_asa_reserves=current_asa_reserves,
                                                       current_mualgo_reserves=current_mualgo_reserves,
                                                       t=time,
                                                       current_mualgo_position=self.pos_impact_state.mualgo_position,
                                                       slippage=0)

            if opt_swap_quote is not None:

                validate_sim_swap(opt_swap_quote, current_mualgo_reserves, current_asa_reserves, self.pos_impact_state,
                                  asset_id, time)
                traded_swap = self.swapper[asset_id].attempt_transaction(opt_swap_quote)
                self.pos_impact_state.update(
                    asset_id,
                    traded_swap,
                    current_mualgo_reserves,
                    current_asa_reserves,
                    time
                )

                if traded_swap.asset_buy == 0:
                    price_other = current_asa_reserves / current_mualgo_reserves
                else:
                    price_other = current_mualgo_reserves / current_asa_reserves
                asa_impact = self.pos_impact_state.asa_states[asset_id].impact.value(time)

                if opt_swap_quote.amount_out.asset.id == 0:
                    out_reserves = current_mualgo_reserves
                elif opt_swap_quote.amount_out.asset.id > 0:
                    out_reserves = current_asa_reserves
                else:
                    raise ValueError

                trade_costs = TradeCostsOther(buy_asset=traded_swap.asset_buy,
                                              buy_amount=traded_swap.amount_buy,
                                              buy_reserves=out_reserves,
                                              buy_asset_price_other=price_other,
                                              asa_impact=asa_impact).to_mualgo_basis()
                trade_record = TradeRecord(
                    time=time,
                    asset_buy_id=opt_swap_quote.amount_out.asset.id,
                    asset_sell_id=opt_swap_quote.amount_in.asset.id,
                    asset_buy_amount=opt_swap_quote.amount_out.amount,
                    asset_sell_amount=opt_swap_quote.amount_in.amount
                )
                trade_info = TradeInfo(trade_record, trade_costs, current_mualgo_reserves / current_asa_reserves)
                log_trade(trade_info)

                if self.log_state:
                    self.log_state(self.pos_impact_state)

            elif self.log_null_trades:
                trade_costs = TradeCostsMualgo.zero()
                trade_record = TradeRecord.zero(time, 0, asset_id)
                trade_info = TradeInfo(trade_record, trade_costs,
                                       asa_price=current_mualgo_reserves / current_asa_reserves)
                log_trade(trade_info)

    def update_market_state(self, time, asset_id, price_update):
        self.prices[asset_id] = price_update
        asa_price_mualgo = price_update.asset2_reserves / price_update.asset1_reserves
        self.signal_providers[asset_id].update(time, asa_price_mualgo)

    def run(self, end_time: datetime.datetime, log_trade: Callable[[TradeInfo], None]):

        # Takes values on the times where we run the trading loop
        current_time: Optional[datetime.datetime] = None
        initial_time: Optional[datetime.datetime] = None

        for x in self.price_stream:
            self.logger.debug(f'{x}')
            assert x.asset_ids[1] == 0
            asset_id, price_update = x.asset_ids[0], x.price_update
            # Time of the price update
            time = int_to_tzaware_utc_datetime(x.price_update.time)

            if initial_time is None:
                initial_time = time
                current_time = time
            assert time >= current_time

            while time - current_time > self.simulation_step:
                current_time = current_time + self.simulation_step
                # Trade only if we are not seeding
                if time - initial_time > self.seed_time:
                    self.logger.debug(f'Entering trading loop at sim time {current_time}')
                    self.trade_loop(current_time, log_trade)
                else:
                    self.logger.debug(f'Still seeding at sim time {current_time}')

            # End the simulation
            if time > end_time:
                break

            self.update_market_state(time, asset_id, price_update)
