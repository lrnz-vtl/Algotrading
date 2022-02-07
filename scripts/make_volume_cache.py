import datetime
from algo.blockchain.process_volumes import VolumeCacher
from tinyman.v1.client import TinymanMainnetClient
import argparse

if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument('-u', dest='universe_cache_name', type=str, required=True)
        parser.add_argument('-c', dest='cache_name', type=str, required=True)

        args = parser.parse_args()

        date_min = datetime.datetime(year=2022, month=1, day=20)

        client = TinymanMainnetClient()
        pc = VolumeCacher(
                client=client,
                universe_cache_name=args.universe_cache_name,
                date_min=date_min,
                date_max=None
        )
        pc.cache(args.cache_name)

