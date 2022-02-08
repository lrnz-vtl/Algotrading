import datetime
from algo.blockchain.process_volumes import VolumeCacher
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient
import argparse

if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('-u', dest='universe_cache_name', type=str, required=True)
        parser.add_argument('-c', dest='cache_name', type=str, required=True)

        args = parser.parse_args()

        date_min = datetime.datetime(year=2022, month=1, day=20)
        date_max = datetime.datetime(year=2021, month=12, day=31)

        client = TinymanOldnetClient()
        pc = VolumeCacher(
                client=client,
                cache_file=args.universe_cache_name,
                date_min=date_min,
                date_max=None
        )
        pc.cache(args.cache_name)

