import requests
import json
from dataclasses import dataclass
from typing import Optional

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
            raise Exception(json.dumps(tx,indent=4)) from e


pool_address = 'XLNMBK3GMC4YEF562DMEOZEYTNDWJLQRN2GFQ3MGLVNIJTOTVGVD75DVKM'

# Lorenzo's address
address = 'E5SDQTVXEKCXRUW35MFUOC6PBT62COIGPOW44ISCLR2YV3WA6ZQURZR5DI'
address = 'WSGJWJCCM7Y7IYFYG3VQKB65TECJKRLIC5GQ6WYUHVKOH4YDBMSQY3HKHI'

# for tx in query_transactions({'address':address, 'round':18970337}):
#     if tx['tx-type'] == 'axfer':
#         # ASA
#         key = 'asset-transfer-transaction'
#         asset_id = tx[key]['asset-id']
#         receiver = tx[key]['receiver']
#     elif tx['tx-type'] == 'pay':
#         # Algo
#         key = 'payment-transaction'
#         asset_id = 0
#         receiver = tx[key]['receiver']
#     elif tx['tx-type'] == 'appl':
#         receiver = address
#         key = 'local-state-delta'
#     else:
#         continue
#
#     sender = tx['sender']
#     keys = [key, 'confirmed-round', 'sender', 'tx-type']
#     if pool_address in (receiver, sender):
#         print(json.dumps({key: tx[key] for key in keys}, indent=4))


# for tx in query_transactions({'address': address, 'round': 18970337, 'tx-type': 'appl'}):
#     if tx['tx-type'] == 'axfer':
#         # ASA
#         key = 'asset-transfer-transaction'
#         asset_id = tx[key]['asset-id']
#
#     elif tx['tx-type'] == 'pay':
#         # Algo
#         key = 'payment-transaction'
#         asset_id = 0
#     else:
#         pass
#
#     print(json.dumps(tx, indent=4))


# Defly - Algo Pool
#
for tx in query_transactions_for_pool(pool_address):
#     if tx.counterparty == 'WSGJWJCCM7Y7IYFYG3VQKB65TECJKRLIC5GQ6WYUHVKOH4YDBMSQY3HKHI' and tx.block == 18970337:
    print(tx)