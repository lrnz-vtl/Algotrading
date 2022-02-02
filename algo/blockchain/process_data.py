import requests
import json
from dataclasses import dataclass
from typing import Optional
from tinyman.v1.client import TinymanClient, TinymanMainnetClient


@dataclass
class PoolTransaction:
    amount: int
    asset_id: int
    block: int
    counterparty: str


def query_transactions(params):
    query = f'https://algoindexer.algoexplorerapi.io/v2/transactions'
    resp = requests.get(query, params=params).json()
    for tx in resp['transactions']:
        yield tx


def query_transactions_for_pool(pool_address: str):
    for tx in query_transactions({'address': pool_address}):

        try:
            if tx['tx-type'] == 'axfer':
                # ASA
                key = 'asset-transfer-transaction'
                asset_id = tx[key]['asset-id']

            elif tx['tx-type'] == 'pay':
                # Algo
                key = 'payment-transaction'
                asset_id = 0
            else:
                continue

            receiver, sender = tx[key]['receiver'], tx['sender']

            if pool_address == receiver:
                counterparty = sender
                sign = +1
            elif pool_address == sender:
                counterparty = receiver
                sign = -1
            else:
                raise ValueError(f'pool_address {pool_address} neither in sender nor receiver')

            amount = sign * tx[key]['amount']
            block = tx['confirmed-round']
            yield PoolTransaction(amount, asset_id, block, counterparty)

        except Exception as e:
            raise Exception(json.dumps(tx, indent=4)) from e


pool_address = 'XLNMBK3GMC4YEF562DMEOZEYTNDWJLQRN2GFQ3MGLVNIJTOTVGVD75DVKM'




# Logged swap for a pool, excluding redeeming amounts
@dataclass
class Swap:
    # Asset id going to the pool
    asset_in: int
    # Amount going the pool
    amount_in: int
    # Asset id going to the counterparty
    asset_out: int
    # Amount going to the counterparty
    amount_out: int
    counterparty: str
    block: int


# TODO Check this is valid, does it also hold for pools without Algo?
def is_fee_payment(tx: PoolTransaction):
    return tx.asset_id == 0 and tx.amount == 2000


class SwapScraper:
    def __init__(self, asset1_id, asset2_id):

        client = TinymanMainnetClient()
        pool = client.fetch_pool(asset1_id, asset2_id)
        assert pool.exists

        self.liquidity_asset = pool.liquidity_asset.id
        self.assets = [asset1_id, asset2_id]
        self.address = pool.address

    def scrape(self):

        def is_transaction_in(tx: PoolTransaction, transaction_out: PoolTransaction):
            return tx.counterparty == transaction_out.counterparty \
                   and tx.asset_id != transaction_out.asset_id \
                   and tx.asset_id in self.assets \
                   and not is_fee_payment(tx)

        transaction_out: Optional[PoolTransaction] = None
        transaction_in: Optional[PoolTransaction] = None

        for tx in query_transactions_for_pool(self.address):

            if transaction_out:
                # We recorded a transaction out and in, looking for a fee payment
                if transaction_in:
                    if is_fee_payment(tx) and tx.counterparty == transaction_in.counterparty:
                        yield Swap(asset_in=transaction_in.asset_id,
                                   asset_out=transaction_out.asset_id,
                                   amount_in=transaction_in.amount,
                                   amount_out=-transaction_out.amount,
                                   counterparty=tx.counterparty,
                                   block=tx.block
                                   )
                    transaction_out = None
                    transaction_in = None

                # We recorded a transaction out, looking for a transaction in
                else:
                    # TODO We should account for redeeming excess funds from the pool?
                    if is_transaction_in(tx, transaction_out):
                        transaction_in = tx
                    else:
                        transaction_out = None
            else:
                if tx.amount < 0 and tx.asset_id in self.assets:
                    transaction_out = tx


sc = SwapScraper(0, 470842789)
for tx in sc.scrape():
    print(tx)