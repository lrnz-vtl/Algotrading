import datetime
import logging
from algo.universe.universe import PoolIdStore
from algo.blockchain.process_prices import PriceCacher
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='poolidstore_cache_name', type=str, required=True)
    parser.add_argument('-c', dest='cache_name', type=str, required=True)

    args = parser.parse_args()

    date_min = datetime.datetime(year=2021, month=10, day=8)
    date_max = datetime.datetime(year=2021, month=12, day=31)

    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO)

    ps = PoolIdStore.from_cache(args.poolidstore_cache_name)

    pc = PriceCacher(client=TinymanOldnetClient(),
                     pool_id_store=ps,
                     date_min=date_min,
                     date_max=date_max
                     )
    pc.cache(args.cache_name)

