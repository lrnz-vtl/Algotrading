import datetime
from algo.blockchain.process_prices import PriceCacher
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', dest='universe_cache_name', type=str, required=True)
    parser.add_argument('-c', dest='cache_name', type=str, required=True)

    args = parser.parse_args()

    date_min = datetime.datetime(year=2021, month=10, day=8)
    date_max = datetime.datetime(year=2021, month=12, day=31)

    pc = PriceCacher(client=TinymanOldnetClient(),
                     cache_file=args.universe_cache_name,
                     date_min=date_min,
                     date_max=date_max
                     )
    pc.cache(args.cache_name)

