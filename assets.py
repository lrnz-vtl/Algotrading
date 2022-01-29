import logging

import algosdk.error
import requests
from tinyman.v1.client import TinymanClient
from typing import Optional
from functools import lru_cache

# Most liquid assets
assets = [31566704, 226701642, 523683256, 287867876, 230946361,
          137594422, 310014962, 378382099, 137020565, 297995609, 470842789]


# assets = [163650, 283820866, 31566704, 226701642, 320259224, 27165954,
#           312769, 300208676, 384303832, 287867876, 230946361,
#           137594422, 310014962, 378382099, 137020565, 297995609,
#           241759159]

class Universe:
    def __init__(self, client:Optional[TinymanClient], check_pairs:bool = False):
        self.client = client
        self.check_pairs = check_pairs
        self.logger = logging.getLogger("Universe")
        self.logger.setLevel(logging.INFO)

        if check_pairs:
            assert self.client is not None

        self.logger.info("Finding viable pools...")
        self.pools = self._find_pools()

    @lru_cache()
    def _check_existing(self, asset_id: int) -> bool:
        # This is just Algo
        if asset_id == 0:
            return True
        try:
            self.client.algod.asset_info(asset_id)
            return True
        except algosdk.error.AlgodHTTPError as e:
            self.logger.info(f"Skipping asset {asset_id} because it does not exist: code= {e.code}")
            self.logger.info(e)
            return False

    def _check_pool(self, p0:int, p1:int):
        try:
            return self.client.fetch_pool(p0, p1).exists
        except KeyError as e:
            self.logger.info(f"Skipping pool ({p0,p1}) because received KeyError with key {e}")
            return False

    def _find_pools(self) -> list[tuple[int, int]]:

        throw = [403996358, 533933723, 559276904]

        pools = set()

        res = requests.get(url='https://mainnet.analytics.tinyman.org/api/v1/pools/').json()
        while res['next']:
            for p in res['results']:

                p0, p1 = int(p['asset_1']['id']), int(p['asset_2']['id'])
                p0, p1 = min(p0, p1), max(p0, p1)

                if (p0 not in throw and p1 not in throw) \
                        and ((p0, p1) not in pools)\
                        and (not self.check_pairs or (self._check_existing(p0) and self._check_existing(p1) and self._check_pool(p0, p1))):
                    pools.add((p0,p1))

            res = requests.get(url=res['next']).json()

        return list(pools)


def get_name(asset_id):
    """Return the name and ticker for a given asset id"""
    return {
        0: ('Algorand', 'ALGO'),
        163650: ('Asia Reserve Currency Coin', 'ARCC'),
        283820866: ('Xfinite Entertainment Token', 'XET'),
        31566704: ('USDC', 'USDC'),
        226701642: ('Yieldly', 'YLDY'),
        320259224: ('Wrapped Algo', 'wALGO'),
        27165954: ('PLANET', 'Planets'),
        312769: ('Tether USDt', 'USDt'),
        300208676: ('Smile Coin', 'SMILE'),
        523683256: ('AKITA INU', 'AKTA'),
        #        384303832: ('AKITA INU TOKEN', 'AKITA'),
        287867876: ('Opulous', 'OPUL'),
        230946361: ('AlgoGems', 'GEMS'),
        137594422: ('HEADLINE', 'HDL'),
        310014962: ('AlcheCoin', 'ALCH'),
        378382099: ('Tinychart Token', 'TINY'),
        137020565: ('Buy Token', 'BUY'),
        297995609: ('Choice Coin', 'Choice'),
        241759159: ('Freckle', 'FRKL'),
        470842789: ('Defly Token', 'DEFLY')
    }.get(asset_id, (None, None))


verified_assets = [0, 163650, 265122, 312769, 438828, 438831, 438832, 438833, 438836, 438837, 438838, 438839, 438840,
                   2350276, 2512768, 2513338, 2513746, 2514157, 2751733, 2757561, 2836760, 2838934, 6547014, 6587142,
                   27165954, 31566704, 83209012, 84507107, 112866019, 125584116, 127494380, 135464366, 137020565,
                   137594422, 142838028, 143787817, 181380658, 187215017, 197112469, 200730915, 213345970, 226265212,
                   226701642, 227855942, 230946361, 231880341, 233939122, 237913743, 239444645, 241759159, 246516580,
                   246519683, 251014570, 257805044, 259535809, 263891752, 263893023, 264229768, 272839935, 276461096,
                   281003266, 281003863, 281004528, 281005704, 283820866, 284090786, 287504952, 287867876, 291248873,
                   297995609, 300208676, 305992851, 306034694, 307329013, 310014962, 310079703, 311714745, 317264620,
                   317670428, 319473667, 320259224, 327821015, 329110405, 330168845, 338543684, 340987160, 342889824,
                   352658929, 353409462, 361339277, 361671874, 361806984, 361940410, 362992028, 363833896, 364187398,
                   364251975, 366511140, 367029007, 370073176, 371035527, 373932709, 383581973, 384513011, 386192725,
                   386195940, 388502764, 388592191, 391379500, 392693339, 393155456, 393537671, 394014424, 394412320,
                   396841550, 403499324, 405565155, 412056867, 416737271, 426980914, 433100599, 435335235, 441139422,
                   445907756, 445924570, 445925266, 445935868, 452047208, 453785660, 453816186, 456473987, 457205263,
                   457819394, 461849439, 462629820, 463554836, 465818547, 465818553, 465818554, 465818555, 465818563,
                   465865291, 466872875, 467134640, 478549868, 509808838, 511484048, 522698212]

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

decimals = {0: 0, 163650: 6, 265122: 3, 312769: 6, 438828: 6, 438831: 6, 438832: 0, 438833: 0, 438836: 0, 438837: 0,
            438838: 0, 438839: 0, 438840: 0, 2350276: 6, 2512768: 6, 2513338: 6, 2513746: 6, 2514157: 6, 2751733: 7,
            2757561: 7, 2836760: 0, 2838934: 0, 6547014: 5, 6587142: 5, 27165954: 6, 31566704: 6, 83209012: 8,
            84507107: 2, 112866019: 4, 125584116: 3, 127494380: 4, 135464366: 6, 137020565: 2, 137594422: 6,
            142838028: 2, 143787817: 0, 181380658: 2, 187215017: 2, 197112469: 6, 200730915: 0, 213345970: 8,
            226265212: 0, 226701642: 6, 227855942: 6, 230946361: 6, 231880341: 7, 233939122: 0, 237913743: 0,
            239444645: 0, 241759159: 0, 246516580: 6, 246519683: 6, 251014570: 0, 257805044: 0, 259535809: 0,
            263891752: 6, 263893023: 6, 264229768: 2, 272839935: 6, 276461096: 0, 281003266: 0, 281003863: 0,
            281004528: 0, 281005704: 0, 283820866: 9, 284090786: 0, 287504952: 0, 287867876: 10, 291248873: 0,
            297995609: 2, 300208676: 6, 305992851: 3, 306034694: 0, 307329013: 0, 310014962: 0, 310079703: 0,
            311714745: 6, 317264620: 0, 317670428: 10, 319473667: 5, 320259224: 6, 327821015: 0, 329110405: 0,
            330168845: 2, 338543684: 5, 340987160: 8, 342889824: 6, 352658929: 0, 353409462: 4, 361339277: 2,
            361671874: 5, 361806984: 0, 361940410: 0, 362992028: 2, 363833896: 5, 364187398: 3, 364251975: 0,
            366511140: 0, 367029007: 0, 370073176: 1, 371035527: 0, 373932709: 2, 383581973: 6, 384513011: 0,
            386192725: 8, 386195940: 8, 388502764: 6, 388592191: 1, 391379500: 6, 392693339: 4, 393155456: 2,
            393537671: 6, 394014424: 0, 394412320: 3, 396841550: 6, 403499324: 0, 405565155: 5, 412056867: 6,
            416737271: 0, 426980914: 2, 433100599: 2, 435335235: 0, 441139422: 6, 445907756: 2, 445924570: 2,
            445925266: 2, 445935868: 2, 452047208: 6, 453785660: 3, 453816186: 6, 456473987: 6, 457205263: 2,
            457819394: 6, 461849439: 3, 462629820: 6, 463554836: 6, 465818547: 6, 465818553: 6, 465818554: 8,
            465818555: 8, 465818563: 6, 465865291: 6, 466872875: 6, 467134640: 6, 478549868: 0, 509808838: 6,
            511484048: 2, 522698212: 1}
