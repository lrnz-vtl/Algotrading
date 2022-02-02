import logging
from algo.universe.pools import PoolInfoStore, PoolInfoStoreScratchInputs
from tinyman.v1.client import TinymanMainnetClient
import argparse
import time

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch and serialise candidate assets.')
    parser.add_argument('-c', dest='cache_name', type=str, required=True)
    parser.add_argument('-l', dest='query_limit', type=int, required=False, help='How many queries to send the API (10 results per query)')

    args = parser.parse_args()

    timestr = time.strftime("%Y%m%d-%H%M%S")
    cache_name = timestr

    client = TinymanMainnetClient()
    inputs = PoolInfoStoreScratchInputs(client=client, query_limit=args.query_limit)
    ps = PoolInfoStore(inputs=inputs)

    ps.serialize(args.cache_name)
