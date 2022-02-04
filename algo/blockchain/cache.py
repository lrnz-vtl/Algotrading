from algo.universe.universe import Universe
from tinyman.v1.client import TinymanMainnetClient
from algo.blockchain.process_prices import PriceScraper
from algo.blockchain.utils import datetime_to_int, generator_to_df
import pyarrow as pa
import pyarrow.parquet as pq
from definitions import ROOT_DIR
import numpy as np
import os
import pandas as pd
import datetime
from pathlib import Path
import glob
pd.options.mode.chained_assignment = None  # default='warn'

PRICE_CACHES_BASEDIR = f'{ROOT_DIR}/caches/prices'


class PriceDataCacher:
    def __init__(self, universe_cache_name: str, date_min: datetime):
        self.client = TinymanMainnetClient()
        self.pools = [(x.asset1_id, x.asset2_id) for x in Universe.from_cache(universe_cache_name).pools]
        self.date_min = date_min
        assert date_min == datetime.datetime(year=date_min.year, month=date_min.month, day=date_min.day), \
            f"date_min = {date_min} argument must be date without hours, minutes etc."

    def cache(self, cache_name: str):
        basedir = os.path.join(PRICE_CACHES_BASEDIR, cache_name)
        os.makedirs(basedir, exist_ok=True)

        for assets in self.pools:
            self._cache_pool(assets, basedir)

    def _cache_pool(self, assets, basedir):
        assets = list(sorted(assets))
        cache_dir = os.path.join(basedir, "_".join([str(x) for x in assets]))
        os.makedirs(cache_dir, exist_ok=True)

        def file_name(date):
            return os.path.join(cache_dir, f'{date}.parquet')

        def path_to_date(fname):
            datestr = Path(fname).name.split('.')[0]
            date = datetime.datetime.strptime(datestr, '%Y-%m-%d')
            return date

        existing_dates = {path_to_date(fname) for fname in glob.glob(f'{cache_dir}/*.parquet')}

        today_date = datetime.datetime.today()
        date_min = self.date_min
        while date_min < today_date:
            if date_min not in existing_dates:
                break
            date_min = date_min + datetime.timedelta(days=1)
        print(f'Found minimun date to scrape for assets {assets[0], assets[1]} = {date_min}')

        pc = PriceScraper(self.client, assets[0], assets[1])
        df = generator_to_df(pc.query_pool_state_history(num_queries=None,
                                                         timestamp_min=datetime_to_int(date_min))
                             )

        def cache_if_new(daydf: pd.DataFrame, date):
            fname = file_name(date)
            if os.path.exists(fname):
                pass
            else:
                table = pa.Table.from_pandas(daydf)
                pq.write_table(table, fname)

        dates = df['time'].dt.date

        # Exclude the current day as it's not complete
        full_days = list(dates.sort_values().unique()[:-1])

        idx = dates.isin(full_days)
        selected_dates = dates[idx]
        df_to_cache = df[dates.isin(full_days)]
        df_to_cache['time'] = df_to_cache['time'].view(dtype=np.int64) // 1000000000

        df_to_cache.groupby(selected_dates).apply(lambda x: cache_if_new(x, x.name))
