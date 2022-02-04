import requests
from dataclasses import dataclass
from typing import Optional
from algo.blockchain.utils import query_transactions
from base64 import b64decode, b64encode
import warnings
import time


def get_state_int(state, key):
    if type(key) == str:
        key = b64encode(key.encode())
    return state.get(key.decode(), {'uint': None})['uint']


@dataclass
class PoolState:
    time: int
    asset1_reserves: int
    asset2_reserves: int


def get_pool_state(pool_address: str):
    query = f'https://algoindexer.algoexplorerapi.io/v2/accounts/{pool_address}'
    resp = requests.get(query).json()['account']['apps-local-state'][0]
    state = {y['key']: y['value'] for y in resp['key-value']}
    return PoolState(int(time.time()), get_state_int(state, 's1'), get_state_int(state,'s2'))


def get_pool_state_txn(tx: dict):
    if tx['tx-type'] != 'appl':
        warnings.warn('Attempting to extract pool state from non application call')
    state = {x['key'] : x['value'] for x in tx['local-state-delta'][0]['delta']}
    s1 = get_state_int(state, 's1')
    s2 = get_state_int(state, 's2')
    if s1 is None or s2 is None:
        return None
    return PoolState(tx['round-time'], s1, s2)


def query_pool_state_history(pool_address: str, num_queries: Optional[int], timestamp_min: int = 0):
    prev_time = None
    for tx in query_transactions(params={'address': pool_address}, num_queries=num_queries):
        if tx['tx-type'] != 'appl':
            continue
        if tx['round-time'] < timestamp_min:
            break
        ps = get_pool_state_txn(tx)
        if not ps or (prev_time and prev_time == ps.time):
            continue
        prev_time = ps.time
        yield ps
