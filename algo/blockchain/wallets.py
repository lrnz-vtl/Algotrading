from __future__ import annotations
import requests, logging, time, datetime, warnings
import pandas as pd
from typing import Dict, Iterable, Tuple, Optional
from algo.blockchain.algo_requests import QueryParams
from algo.blockchain.stream import DataStream
from algo.universe.universe import SimpleUniverse


def get_asset_data(asset_id, testnet=False):
    if testnet:
        asset = requests.get(url=f'https://testnet.algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    else:
        asset = requests.get(url=f'https://algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    return asset['asset']


def get_decimal(asset_id, testnet=False):
    if testnet:
        asset = requests.get(url=f'https://testnet.algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    else:
        asset = requests.get(url=f'https://algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    return asset['asset']['params']['decimals']


def get_account_data(address=None, testnet=False):
    """Query wallet data from AlgoExplorer"""

    if testnet:
        url = f'https://testnet.algoexplorerapi.io/v2/accounts/{address}'
    else:
        url = f'https://algoexplorerapi.io/v2/accounts/{address}'

    # as specified here https://algoexplorer.io/api-dev/v2
    data = requests.get(url=url).json()

    # set up dictionary with values for each coin
    coins = {0: data['amount']}  # / 10 ** 6}
    for d in data['assets']:
        coins[d['asset-id']] = d['amount']  # / 10 ** get_decimal(d['asset-id'],testnet)

    # return the assets in the wallet
    # note: we are discarding some info here (rewards, offline, frozen, etc)
    return coins


class Portfolio:
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

    def portfolio_value(self, price_df, after_time: Optional[datetime.date] = None, before_time: Optional[datetime.date] = None):
        if after_time == None:
            after_time = datetime.datetime.fromtimestamp(price_df['time'].min())
        if before_time == None:
            before_time = datetime.datetime.fromtimestamp(price_df['time'].max())
        query_params = QueryParams(after_time=after_time)
        wallet = self.historical_numbers(query_params)
        pricedic = {}
        for (asset1, asset2), price in price_df.groupby(['asset1','asset2']):
            if asset2==0:
                pricedic[asset1] = price
        value = []
        invalid = set()
        for t, r in wallet.iterrows():
            val = 0
            for assetid in r.keys():
                if assetid==0:
                    val += r[0]
                    continue
                if assetid not in pricedic:
                    if assetid not in invalid:
                        warnings.warn(f'Skipping asset {assetid} for which no price data was given')
                        invalid.add(assetid)
                    continue
                p = pricedic[assetid]
                q = p[p['time'].gt(t)]['time']
                if len(q)==0:
                    continue
                i=p[p['time'].gt(t)]['time'].index[0]
                val += r[assetid]*p.iloc[i]['asset2_reserves']/p.iloc[i]['asset1_reserves']
            value.append({'time': t, 'value':val})
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
                    txn_delta[assetid] =  txn['asset-transfer-transaction']['amount']
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
