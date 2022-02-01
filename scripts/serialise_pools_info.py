import logging
from algo.universe.pools import PoolInfo, PoolInfoStore, PoolInfoStoreScratchInputs
from tinyman.v1.client import TinymanMainnetClient
import json
import argparse
import time
from pathlib import Path
import os

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch and serialise candidate assets.')
    parser.add_argument('-f', dest='base_folder', type=str, required=True)
    parser.add_argument('-l', dest='query_limit', type=int, required=False, help='How many queries to send the API (10 results per query)')

    args = parser.parse_args()

    timestr = time.strftime("%Y%m%d-%H%M%S")
    base_folder = Path(args.base_folder) / timestr
    os.makedirs(base_folder, exist_ok=True)

    client = TinymanMainnetClient()
    inputs = PoolInfoStoreScratchInputs(client=client, query_limit=args.query_limit)
    ps = PoolInfoStore(inputs=inputs)

    cache_fname = base_folder / 'all_pools.json'

    with open(cache_fname, 'w') as f:
        json.dump(ps.asdicts(), f, indent=4)
