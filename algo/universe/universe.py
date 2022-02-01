from __future__ import annotations
import algosdk.error
from tinyman.v1.client import TinymanClient
from typing import Optional
from functools import lru_cache
import logging
import requests
from dataclasses import dataclass


@dataclass
class PoolInfo:
    asset1_id: int
    asset2_id: int
    current_asset_1_reserves_in_usd: int
    current_asset_2_reserves_in_usd: int
    creation_round: int
    address: str
    current_issued_liquidity_assets: int
    is_verified: bool


class PoolInfoStore:
    def __init__(self, client: Optional[TinymanClient], check_pairs: bool = False, test: bool = False):
        self.client = client
        self.check_pairs = check_pairs
        self.test = test
        self.logger = logging.getLogger("Universe")
        self.logger.setLevel(logging.INFO)

        if check_pairs:
            assert self.client is not None

        self.logger.info("Finding viable pools...")
        self.pairs = self._find_pools(test)

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

    def _find_pools(self, test: bool = False) -> list[tuple[int, int]]:

        # throw = [403996358, 533933723, 559276904]

        pairs = set()

        url = 'https://mainnet.analytics.tinyman.org/api/v1/pools/'

        while url:
            res = requests.get(url).json()

            for p in res['results']:

                p0, p1 = int(p['asset_1']['id']), int(p['asset_2']['id'])
                p0, p1 = min(p0, p1), max(p0, p1)

                if ((p0, p1) not in pairs) \
                        and (not self.check_pairs or (
                        self._check_existing(p0) and self._check_existing(p1) and self._check_pool(p0, p1))):
                    pairs.add((p0, p1))

            if test:
                url = None
            else:
                url = res['next']

        return list(pairs)


all_pairs = [(0, 163650), (0, 312769), (0, 438831), (0, 2751733), (0, 2757561), (0, 6547014), (0, 27165954),
             (0, 31566704), (0, 137020565), (0, 137594422), (0, 142838028), (0, 143787817), (0, 181380658),
             (0, 187215017), (0, 200730915), (0, 226265212), (0, 226701642), (0, 230946361), (0, 233939122),
             (0, 239444645), (0, 241759159), (0, 251014570), (0, 264229768), (0, 281003266), (0, 281003863),
             (0, 283820866), (0, 287867876), (0, 291248873), (0, 297995609), (0, 300208676), (0, 305992851),
             (0, 310014962), (0, 311714745), (0, 319473667), (0, 320259224), (0, 329110405), (0, 338543684),
             (0, 342889824), (0, 352658929), (0, 353409462), (0, 361671874), (0, 361806984), (0, 361940410),
             (0, 363833896), (0, 366511140), (0, 367029007), (0, 383581973), (0, 384513011), (0, 386192725),
             (0, 386195940), (0, 388502764), (0, 388592191), (0, 391379500), (0, 392693339), (0, 393155456),
             (0, 393537671), (0, 394014424), (0, 394412320), (0, 403499324), (0, 412056867), (0, 416737271),
             (0, 426980914), (0, 433100599), (0, 435335235), (0, 452047208), (0, 453816186), (0, 456473987),
             (0, 457205263), (0, 457819394), (0, 461849439), (0, 463554836), (0, 465865291), (0, 467134640),
             (0, 478549868), (0, 511484048), (163650, 438831), (163650, 31566704), (163650, 226701642),
             (312769, 31566704), (312769, 239444645), (312769, 342889824), (312769, 388502764), (2757561, 31566704),
             (27165954, 31566704), (27165954, 137594422), (27165954, 226701642), (27165954, 251014570),
             (27165954, 342889824), (31566704, 226701642), (31566704, 287867876), (31566704, 297995609),
             (31566704, 338543684), (31566704, 342889824), (31566704, 386192725), (31566704, 386195940),
             (31566704, 388502764), (31566704, 393155456), (31566704, 465865291), (31566704, 511484048),
             (137020565, 226701642), (137594422, 226701642), (137594422, 264229768), (137594422, 283820866),
             (137594422, 297995609), (137594422, 388502764), (137594422, 511484048), (226265212, 403499324),
             (226701642, 230946361), (226701642, 283820866), (226701642, 287867876), (226701642, 297995609),
             (226701642, 300208676), (226701642, 342889824), (226701642, 361671874), (226701642, 388502764),
             (226701642, 388592191), (226701642, 426980914), (226701642, 465865291), (226701642, 511484048),
             (230946361, 300208676), (287867876, 297995609), (287867876, 300208676), (287867876, 388502764),
             (297995609, 300208676), (297995609, 319473667), (297995609, 342889824), (297995609, 386192725),
             (297995609, 386195940), (297995609, 403499324), (300208676, 342889824), (300208676, 433100599),
             (300208676, 465865291), (329110405, 361671874), (329110405, 388502764), (329110405, 452047208),
             (353409462, 388502764), (386192725, 386195940), (386192725, 465865291), (388502764, 393537671),
             (388502764, 394412320), (388502764, 511484048), (393155456, 511484048)]
