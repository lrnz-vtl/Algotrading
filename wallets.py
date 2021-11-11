import requests, pprint
import pandas as pd
import numpy as np
from typing import Dict, Iterable, Tuple
from tinydata import TinyData


def get_asset_data(asset_id, testnet=False):
    if testnet:
        asset = requests.get(url=f'https://testnet.algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    else:
        asset = requests.get(url=f'https://algoexplorerapi.io/idx2/v2/assets/{asset_id}').json()
    return asset['asset']


def get_account_data(address=None, testnet=False):
    """Query wallet data from AlgoExplorer"""

    if testnet:
        url = f'https://testnet.algoexplorerapi.io/v2/accounts/{address}'
    else:
        url = f'https://algoexplorerapi.io/v2/accounts/{address}'

    # as specified here https://algoexplorer.io/api-dev/v2
    data = requests.get(url=url).json()

    # set up dictionary with values for each coin
    coins = {0: data['amount'] / 10 ** 6}
    for d in data['assets']:
        coins[d['asset-id']] = d['amount'] / 10 ** get_asset_data(d['asset-id'],testnet)['params']['decimals']

    # return the assets in the wallet
    # note: we are discarding some info here (rewards, offline, frozen, etc)
    return coins


class Portfolio:
    def __init__(self, address=None, testnet=False):
        """Set up the portfolio from a wallet address"""

        self.testnet = testnet

        if not address:
            # set up a dummy portfolio
            self.coins: Dict[int, int] = {0: 100}
        else:
            self.address = address
            self.update()

    def update(self):
        self.coins: Dict[int, int] = get_account_data(self.address, self.testnet)

    def __getitem__(self, asset_id):
        return self.coins[asset_id]

    def __len__(self):
        return len(self.coins)

    def __iter__(self) -> Iterable[Tuple[int, int]]:
        return iter(self.coins)
