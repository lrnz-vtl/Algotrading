from algo.blockchain.process_volumes import PoolTransaction, Swap, is_fee_payment
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
        self.transaction_out = {pool : None for pool in self.pools.keys()}
        self.transaction_in = {pool : None for pool in self.pools.keys()}
        
    def scrape(self):
        
        def is_transaction_in(tx: PoolTransaction, transaction_out: PoolTransaction,
                              asset1_id: int, asset2_id: int):
            return tx.counterparty == transaction_out.counterparty \
                   and tx.asset_id != transaction_out.asset_id \
                   and tx.asset_id in [asset1_id, asset2_id] \
                   and not is_fee_payment(tx)

        
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
                asset1_id = self.pools[pool][0]
                asset2_id = self.pools[pool][1]
                # adapted from algo.blockchain.process_volumes.SwapScraper.scrape
                if self.transaction_out[pool]:
                    if self.transaction_in[pool]:
                        if is_fee_payment(pt) and pt.counterparty == self.transaction_in[pool].counterparty:
                            if self.transaction_in[pool].asset_id == asset1_id \
                               and self.transaction_out[pool].asset_id == asset2_id:
                                asset1_amount = self.transaction_in[pool].amount
                                asset2_amount = self.transaction_out[pool].amount
                            elif self.transaction_in[pool].asset_id == asset2_id \
                                 and self.transaction_out[pool].asset_id == asset1_id:
                                asset2_amount = self.transaction_in[pool].amount
                                asset1_amount = self.transaction_out[pool].amount
                            else:
                                raise ValueError
                            assert self.transaction_in[pool].amount > 0 \
                                and self.transaction_out[pool].amount < 0
                            swap = Swap(asset1_amount=asset1_amount,
                                        asset2_amount=asset2_amount,
                                        counterparty=pt.counterparty,
                                        block=pt.block,
                                        time=pt.time)
                            self.volumes[pool].append(swap)
                        self.transaction_out[pool] = None
                        self.transaction_in[pool] = None
                    else:
                        if is_transaction_in(pt, self.transaction_out[pool], asset1_id, asset2_id):
                            self.transaction_in[pool] = pt
                        else:
                            self.transaction_out[pool] = None
                else:
                    if pt.amount < 0 and pt.asset_id in [asset1_id, asset2_id]:
                        self.transaction_out[pool] = pt
