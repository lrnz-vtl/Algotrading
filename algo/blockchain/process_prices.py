import aiohttp

import requests
from dataclasses import dataclass
from typing import Optional
from algo.blockchain.requests import query_transactions
from base64 import b64decode, b64encode
import warnings
import time
from tinyman.v1.client import TinymanClient
from algo.blockchain.base import DataScraper
from algo.blockchain.cache import DataCacher
from definitions import ROOT_DIR
import datetime


PRICE_CACHES_BASEDIR = f'{ROOT_DIR}/caches/prices'


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

    try:
        state = {x['key'] : x['value'] for x in tx['local-state-delta'][0]['delta']}
    except KeyError as e:
        # Looks like this can have a "global-state-delta" instead
        return None

    s1 = get_state_int(state, 's1')
    s2 = get_state_int(state, 's2')
    if s1 is None or s2 is None:
        return None

    return PoolState(tx['round-time'], s1, s2)


class PriceScraper(DataScraper):
    def __init__(self, client: TinymanClient, asset1_id: int, asset2_id: int):

        pool = client.fetch_pool(asset1_id, asset2_id)

        assert pool.exists
        self.liquidity_asset = pool.liquidity_asset.id

        self.assets = [asset1_id, asset2_id]
        self.address = pool.address

    async def scrape(self, session:aiohttp.ClientSession,
                     timestamp_min: int,
                     before_time:Optional[datetime.datetime],
                     num_queries: Optional[int] = None):
        prev_time = None

        async for tx in query_transactions(session=session,
                                           params={'address': self.address},
                                           num_queries=num_queries,
                                           before_time=before_time):
            if tx['tx-type'] != 'appl':
                continue
            if tx['round-time'] < timestamp_min:
                break
            ps = get_pool_state_txn(tx)
            if not ps or (prev_time and prev_time == ps.time):
                continue
            prev_time = ps.time
            yield ps


class PriceCacher(DataCacher):

    def __init__(self, client:TinymanClient,
                 universe_cache_name: str,
                 date_min: datetime.datetime,
                 date_max: Optional[datetime.datetime]):

        super().__init__(universe_cache_name,
                         PRICE_CACHES_BASEDIR,
                         client,
                         date_min,
                         date_max)

    def make_scraper(self, asset1_id: int, asset2_id: int):
        try:
            return PriceScraper(self.client, asset1_id, asset2_id)
        except AssertionError as e:
            return None
