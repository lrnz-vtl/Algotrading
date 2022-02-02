import logging
from algo.universe.universe import Universe
import argparse
import time

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch and serialise universe of pools for trading and logging.')
    parser.add_argument('-c', dest='poolinfo_cache_name', type=str, required=True,
                        help='Load the candidate pools from this cache')
    parser.add_argument('-n', dest='n_most_liquid', type=int, required=True,
                        help='How many of the most liquid pools to select')

    args = parser.parse_args()

    timestr = time.strftime("%Y%m%d-%H%M%S")
    cache_name = timestr

    universe = Universe.from_poolinfo_cache(args.poolinfo_cache_name, args.n_most_liquid)
    universe.serialize(cache_name)


