import h5py
import requests
import os
from definitions import ROOT_DIR

CHAINDATA_BASEFOLDER = os.path.join(ROOT_DIR, 'chainData')

def query_timestamp(block: int) -> int:
    res = requests.get(f'https://algoindexer.algoexplorerapi.io/v2/blocks/{block}').json()
    return int(res['timestamp'])


class CachingTimestampProvider:
    cache_fname = os.path.join(CHAINDATA_BASEFOLDER, 'timestamps.h5')

    def __init__(self):
        pass

    @staticmethod
    def make_new(block_size: int):
        assert not os.path.exists(CachingTimestampProvider.cache_fname)
        return CachingTimestampProvider()

    def get_timestamps(self, blocks: list[int]):
        max_timestamp = max(blocks)
        min_timestamp = min(blocks)

        with h5py.File(self.cache_fname, 'w') as h5:
            h5



        with open()