import pandas as pd
from algo.blockchain.algo_requests import QueryParams
from algo.universe.universe import SimpleUniverse
from algo.blockchain.stream import PriceVolumeDataStore, PriceVolumeStream, DataStream
from algo.blockchain.utils import load_algo_pools, int_to_datetime
from scripts.make_price_report import price_report
from algo.strategy.analytics import process_market_df
import argparse
import time

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='price_cache_name', type=str, required=True)
    parser.add_argument('-v', dest='volume_cache_name', type=str, required=True)
    parser.add_argument('-u', dest='universe', type=str, required=True)

    args = parser.parse_args()

    price_cache = args.price_cache_name
    volume_cache = args.volume_cache_name

    dfp = load_algo_pools(price_cache, 'prices')
    dfv = load_algo_pools(volume_cache, 'volumes')

    # define the query parameters for the stream from the last timestamp in cache data
    time_start = max(dfv.time.max(), dfp.time.max())
    qp = QueryParams(after_time=int_to_datetime(time_start))

    # get the universe and set up the stream
    universe = SimpleUniverse.from_cache(args.universe)
    pvs = PriceVolumeStream(DataStream(universe, qp))
    pvds = PriceVolumeDataStore(pvs)

    print(f'Scraping data from {int_to_datetime(time_start)} onwards.')
    dfp, dfv = pvds.update(dfp, dfv)

    for i in range(5):
        time.sleep(5)
        ti = time.time()
        dfp, dfv = pvds.update(dfp, dfv)
        print(f'Scraping 5 seconds of data in {time.time() - ti} seconds.')

    df = process_market_df(dfp, dfv)

    print(df)
    price_report(df, 'stream_data.pdf')
