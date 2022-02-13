from algo.blockchain.process_volumes import PoolTransaction
from algo.blockchain.process_prices import PoolState, get_pool_state_txn
from algo.universe.universe import SmallUniverse
from typing import Optional
import requests

def get_pool_transaction_txn(tx: dict, pool_address: str, key: str, asset_id: int):

    receiver, sender = tx[key]['receiver'], tx['sender']

    if pool_address == receiver:
        counterparty = sender
        sign = +1
    elif pool_address == sender:
        counterparty = receiver
        sign = -1
    elif pool_address == tx[key]['close-to']:
        return None
    else:
        raise ValueError(f'pool_address {pool_address} neither in sender nor receiver')

    amount = sign * tx[key]['amount']
    block = tx['confirmed-round']
    return PoolTransaction(amount, asset_id, block, counterparty, tx['tx-type'], tx['round-time'])
    
class DataStream:
    def __init__(self, min_round: int, cache_file: str, next_token: Optional[str] = None):
        self.pools = {x.address for x in SmallUniverse.from_cache(cache_file).pools}
        self.url=f'https://algoindexer.algoexplorerapi.io/v2/transactions'
        self.params = {'min-round': min_round}
        if next_token:
            self.params['next'] = next_token

    def next_transaction(self):
        while True:
            req = requests.get(url=self.url, params=self.params).json()
            for tx in req['transactions']:
                pool = None
                if (tx['sender'] in self.pools):
                    pool = tx['sender']
                elif (tx['tx-type']=='pay' and tx['payment-transaction']['receiver'] in self.pools):
                    pool = tx['payment-transaction']['receiver']
                elif (tx['tx-type']=='axfer' and tx['asset-transfer-transaction']['receiver'] in self.pools):
                    pool = tx['asset-transfer-transaction']['receiver']
                if pool:
                    yield (pool, tx)
            if not 'next-token' in req:
                break
            self.params['next'] = req['next-token']

class PriceVolumeStream:
    def __init__(self, min_round: int, cache_file: str, cache_basedir: Optional[str] = None):
        self.dataStream = DataStream(min_round, cache_file)
        self.pools = {x.address : (x.asset1_id, x.asset2_id) for x in SmallUniverse.from_cache(cache_file).pools}
        self.prices = {pool : list() for pool in self.pools.keys()}
        self.volumes = {pool : list() for pool in self.pools.keys()}
    def scrape(self):
        for pool, tx in self.dataStream.next_transaction():
            pt = None
            if tx['tx-type']=='appl':
                ps = get_pool_state_txn(tx)
                if ps:
                    self.prices[pool].append(ps)
            elif tx['tx-type']=='pay':
                key='payment-transaction'
                pt = get_pool_transaction_txn(tx, pool, key, 0)
            elif tx['tx-type']=='axfer':
                key='asset-transfer-transaction'
                pt = get_pool_transaction_txn(tx, pool, key, tx[key]['asset-id'])
            if pt:
                self.volumes[pool].append(pt)
