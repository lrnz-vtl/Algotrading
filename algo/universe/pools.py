from __future__ import annotations

import json

import algosdk.error
from tinyman.v1.client import TinymanClient
from functools import lru_cache
import logging
import requests
from dataclasses import dataclass
import dataclasses
from typing import Optional, Union


@dataclass
class PoolInfo:
    asset1_id: int
    asset2_id: int
    liquidity_asset_id: int
    current_asset_1_reserves_in_usd: int
    current_asset_2_reserves_in_usd: int
    creation_round: int
    address: str
    current_issued_liquidity_assets: int
    is_verified: bool

    @staticmethod
    def from_query_result(r: dict):
        keys = {'current_asset_1_reserves_in_usd', 'current_asset_2_reserves_in_usd', 'creation_round', 'address',
                'current_issued_liquidity_assets', 'is_verified'}

        return PoolInfo(
            asset1_id=r['asset_1']['id'],
            asset2_id=r['asset_2']['id'],
            liquidity_asset_id=r['liquidity_asset']['id'],
            **{key: r[key] for key in keys}
        )


@dataclass
class PoolInfoStoreScratchInputs:
    client: TinymanClient
    query_limit: Optional[int]


PoolInfoStoreInputs = Union[PoolInfoStoreScratchInputs, dict]


class PoolInfoStore:
    """ Needs refactoring """

    def __init__(self, inputs):

        self.logger = logging.getLogger("Universe")
        self.logger.setLevel(logging.INFO)

        if isinstance(inputs, PoolInfoStoreScratchInputs):
            self.client = inputs.client
            self.query_limit = inputs.query_limit
            self.logger.info("Finding viable pools...")
            self.pools = self._find_pools(self.query_limit)

        elif isinstance(inputs, dict):
            self.logger.info("Loading from cache")
            self.client = None
            self.query_limit = inputs['query_limit']
            self.pools = [PoolInfo.from_query_result(x) for x in inputs['pools']]

    @staticmethod
    def from_cache(fname:str) -> PoolInfoStore:
        with open(fname) as f:
            data = json.load(f)

        return PoolInfoStore(data)

    @lru_cache()
    def _check_existing(self, asset_id: int) -> bool:
        # This is just Algo
        if asset_id == 0:
            return True
        try:
            self.client.algod.asset_info(asset_id)
            return True
        except algosdk.error.AlgodHTTPError as e:
            if e.code == 404:
                self.logger.info(f"Skipping asset {asset_id} because it does not exist.")
                return False
            else:
                raise e

    @lru_cache()
    def _check_pool(self, p0: int, p1: int):
        try:
            return self.client.fetch_pool(p0, p1).exists
        except KeyError as e:
            self.logger.warning(f"Skipping pool ({p0, p1}) because received KeyError with key {e}")
            return False

    @staticmethod
    def _get_data(query_limit: Optional[int]) -> list[dict]:
        """
        Try to get data fast because results are paginated and may change quickly
        """

        url = 'https://mainnet.analytics.tinyman.org/api/v1/pools/?ordering=-liquidity'
        ret = []
        i = 0
        while url is not None:
            if query_limit is not None and i >= query_limit:
                break
            res = requests.get(url).json()
            ret += res['results']
            url = res['next']
            i += 1
        return ret

    def _find_pools(self, query_limit: Optional[int]) -> list[PoolInfo]:

        pairs = set()
        pools = []

        for p in self._get_data(query_limit):

            p0, p1 = int(p['asset_1']['id']), int(p['asset_2']['id'])
            p0, p1 = min(p0, p1), max(p0, p1)

            if ((p0, p1) not in pairs) \
                    and self._check_existing(p0) \
                    and self._check_existing(p1) \
                    and self._check_pool(p0, p1):
                pairs.add((p0, p1))
                pools.append(PoolInfo.from_query_result(p))

        return pools

    def asdicts(self):
        return {'client': type(self.client).__name__,
                'query_limit': self.query_limit,
                'pools': [dataclasses.asdict(x) for x in self.pools]}
