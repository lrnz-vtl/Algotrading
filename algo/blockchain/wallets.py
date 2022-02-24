from __future__ import annotations
import time, datetime, warnings
import pandas as pd
from typing import Dict, Iterable, Tuple, Optional
from algo.blockchain.algo_requests import QueryParams
from algo.blockchain.stream import DataStream
from algo.strategy.analytics import ffill_cols, timestamp_to_5min
from algo.tools.wallets import get_account_data


class Wallet:
    def __init__(self, address=None, testnet=False):
        """Set up the portfolio from a wallet address"""

        self.address = address
        self.testnet = testnet

        if not address:
            # set up a dummy portfolio
            self.coins: Dict[int, int] = {0: 1_000_000}
        else:
            self.address = address
            self.update()

    def portfolio_value(self, price_df, after_time: Optional[datetime.date] = None):
        def fill_time(df, m):
            keys = df.keys()
            df['time_5min'] = timestamp_to_5min(df.index)
            all_times = []
            delta = df['time_5min'].max() - df['time_5min'].min()
            for i in range(int(delta.total_seconds() / (5 * 60))):
                all_times.append(df['time_5min'].min() + i * datetime.timedelta(seconds=5 * 60))
            all_times = pd.Series(all_times).rename('time_5min')
            assert len(all_times) == len(set(all_times))
            res = ffill_cols(df, keys, m, all_times)
            res['index'] = res['time_5min'].apply(datetime.datetime.timestamp)
            return res.set_index('index')

        if after_time == None:
            after_time = datetime.datetime.fromtimestamp(price_df['time'].min())
        query_params = QueryParams(after_time=after_time)
        wallet = self.historical_numbers(query_params)
        keys = wallet.keys()
        wallet = fill_time(wallet, 5)
        pricedic = {}
        for (asset1, asset2), price in price_df.groupby(['asset1', 'asset2']):
            if asset2 == 0:
                pricedic[asset1] = price
        value = []
        invalid = set()
        for t, r in wallet[::-1].iterrows():
            val = 0
            for assetid in keys:
                if assetid == 0:
                    val += r[0]
                    continue
                if assetid not in pricedic:
                    if assetid not in invalid:
                        warnings.warn(f'Skipping asset {assetid} for which no price data was given')
                        invalid.add(assetid)
                    continue
                p = pricedic[assetid]
                q = p[p['time'].gt(t)]['time']
                if len(q) == 0:
                    continue
                i = p[p['time'].gt(t)]['time'].index[0]
                val += r[assetid] * p.iloc[i]['asset2_reserves'] / p.iloc[i]['asset1_reserves']
            value.append({'time': t, 'value': val})
        return pd.DataFrame(value)

    def historical_numbers(self, query_params):
        datastream = DataStream.from_address(self.address, query_params)

        self.update()
        assets = [dict(self.coins)]
        assets[0]['time'] = int(time.time())
        for _, txn in datastream.next_transaction():
            txn_delta = {}
            if txn['tx-type'] == 'pay':
                if txn['payment-transaction']['receiver'] == self.address:
                    txn_delta[0] = - txn['payment-transaction']['amount']
                elif txn['sender'] == self.address:
                    txn_delta[0] = txn['payment-transaction']['amount'] + txn['fee']
                else:
                    raise ValueError('encountered invalid transaction')
            elif txn['tx-type'] == 'axfer':
                assetid = txn['asset-transfer-transaction']['asset-id']
                if txn['asset-transfer-transaction']['receiver'] == self.address:
                    txn_delta[assetid] = - txn['asset-transfer-transaction']['amount']
                elif txn['sender'] == self.address:
                    txn_delta[0] = txn['fee']
                    txn_delta[assetid] = txn['asset-transfer-transaction']['amount']
                else:
                    raise ValueError('encountered invalid transaction')
            else:
                if txn['sender'] == self.address:
                    txn_delta[0] = txn['fee']
            if txn_delta:
                txn_delta['time'] = txn['round-time']
                assets.append(txn_delta)
        res = pd.DataFrame(assets).fillna(0)

        return res.set_index('time').cumsum()

    def update(self):
        self.coins: Dict[int, int] = get_account_data(self.address, self.testnet)

    def __getitem__(self, asset_id):
        return self.coins[asset_id]

    def __len__(self):
        return len(self.coins)

    def __iter__(self) -> Iterable[Tuple[int, int]]:
        return iter(self.coins)
