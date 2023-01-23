import datetime
from algo.blockchain.process_prices import PriceCacher
from tinyman.v1.client import TinymanMainnetClient
from algo.universe.pools import PoolIdStore
import argparse
import logging

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='poolidstore_cache_name', type=str, required=True)
    parser.add_argument('-c', dest='cache_name', type=str, required=True)
    parser.add_argument('--dry_run', dest='dry_run', required=False, action='store_true')
    parser.add_argument('--dest_cache', dest='dest_cache', required=False, type=str)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO)

    args = parser.parse_args()

    dry_run = args.dry_run
    dest_cache = args.dest_cache

    if dest_cache is None:
        dest_cache = args.cache_name

    date_min = datetime.datetime(year=2022, month=1, day=20)

    ps = PoolIdStore.from_cache(args.poolidstore_cache_name)

    pc = PriceCacher(client=TinymanMainnetClient(),
                     pool_id_store=ps,
                     date_min=date_min,
                     date_max=None,
                     dry_run=dry_run
                     )
    pc.cache(args.cache_name, dest_cache)

