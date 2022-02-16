import logging
import aiohttp
import requests
from dataclasses import dataclass
from typing import Optional
from algo.blockchain.algo_requests import query_transactions, QueryParams
from base64 import b64decode, b64encode
import warnings
import time
from tinyman.v1.client import TinymanClient
from algo.blockchain.base import DataScraper, NotExistentPoolError
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
    reverse_order_in_block: int

def get_pool_state(pool_address: str):
    query = f'https://algoindexer.algoexplorerapi.io/v2/accounts/{pool_address}'
    resp = requests.get(query).json()['account']['apps-local-state'][0]
    state = {y['key']: y['value'] for y in resp['key-value']}
    return PoolState(int(time.time()), get_state_int(state, 's1'), get_state_int(state,'s2'))


def get_pool_state_txn(tx: dict, prev_time: Optional[int] = None, prev_reverse_order_in_block: Optional[int] = None):
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

    if prev_time and prev_time==tx['round-time']:
        reverse_order_in_block = prev_reverse_order_in_block+1
    else:
        reverse_order_in_block = 0
    
    return PoolState(tx['round-time'], s1, s2, reverse_order_in_block)


class PriceScraper(DataScraper):
    def __init__(self, client: TinymanClient, asset1_id: int, asset2_id: int,
                 skip_same_time: bool = False):

        self.logger = logging.getLogger("PriceScraper")

        pool = client.fetch_pool(asset1_id, asset2_id)

        if not pool.exists:
            raise NotExistentPoolError()
        self.liquidity_asset = pool.liquidity_asset.id

        self.assets = [asset1_id, asset2_id]
        self.address = pool.address
        self.skip_same_time = skip_same_time

    async def scrape(self, session: aiohttp.ClientSession,
                     timestamp_min: int,
                     query_params: QueryParams,
                     num_queries: Optional[int] = None):
        prev_time = None
        prev_reverse_order_in_block = None

        self.logger.debug(f'Started scraping price for assets {self.assets}')

        async for tx in query_transactions(session=session,
                                           params={'address': self.address, 'tx-type': 'appl'},
                                           num_queries=num_queries,
                                           query_params=query_params):

            self.logger.debug(f'Received transaction for assets {self.assets}, '
                              f'block_time={datetime.datetime.fromtimestamp(tx["round-time"])}')

            if tx['tx-type'] != 'appl':
                continue
            if tx['round-time'] < timestamp_min:
                break
            ps = get_pool_state_txn(tx, prev_time, prev_reverse_order_in_block)
            if not ps or (self.skip_same_time and prev_time and prev_time == ps.time):
                continue
            prev_time = ps.time
            prev_reverse_order_in_block = ps.reverse_order_in_block
            yield ps

        self.logger.debug(f'Stopped scraping price for assets {self.assets}')


class PriceCacher(DataCacher):

    def __init__(self, client:TinymanClient,
                 cache_file: str,
                 date_min: datetime.datetime,
                 date_max: Optional[datetime.datetime]):

        super().__init__(cache_file,
                         PRICE_CACHES_BASEDIR,
                         client,
                         date_min,
                         date_max)

    def make_scraper(self, asset1_id: int, asset2_id: int):
        try:
            return PriceScraper(self.client, asset1_id, asset2_id)
        except NotExistentPoolError as e:
            return None
