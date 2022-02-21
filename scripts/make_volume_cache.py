import datetime
from algo.blockchain.process_volumes import VolumeCacher
from tinyman.v1.client import TinymanMainnetClient
from algo.universe.universe import PoolIdStore
import argparse
import logging

if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument('-p', dest='poolidstore_cache_name', type=str, required=True)
        parser.add_argument('-c', dest='cache_name', type=str, required=True)

        args = parser.parse_args()

        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=logging.INFO)

        date_min = datetime.datetime(year=2022, month=1, day=20)

        ps = PoolIdStore.from_cache(args.poolidstore_cache_name)

        client = TinymanMainnetClient()
        pc = VolumeCacher(
                client=client,
                pool_id_store=ps,
                date_min=date_min,
                date_max=None
        )
        pc.cache(args.cache_name)

