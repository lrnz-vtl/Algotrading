import requests, pprint
import pandas as pd
import numpy as np
from typing import Dict, Iterable, Tuple
from tinydata import TinyData

def get_account_data(address=None):
    """Query wallet data from AlgoExplorer"""

    # as specified here https://algoexplorer.io/api-dev/v2
    data = requests.get(url=f'https://algoexplorerapi.io/v2/accounts/{address}').json()

    # set up dictionary with values for each coin
    coins = {0 : data['amount']/10**6}
    for d in data['assets']:        
        decimals = TinyData().asset_summary(d['asset-id'])['decimals']
        coins[d['asset-id']] = d['amount']/10**decimals
        
    # return the assets in the wallet
    # note: we are discarding some info here (rewards, offline, frozen, etc)
    return coins

class Portfolio:
    def __init__(self, address=None):
        """Set up the portfolio from a wallet address"""
        if not address:
            # set up a dummy portfolio
            self.coins: Dict[int, int] = {0: 100}
        else:
            self.address = address
            self.update()

    def update(self):
        self.coins: Dict[int, int] = get_account_data(self.address)

    def __getitem__(self, asset_id):
        return self.coins[asset_id]

    def __len__(self):
        return len(self.coins)

    def __iter__(self) -> Iterable[Tuple[int, int]]:
        return iter(self.coins)
