from algo.universe.universe import Universe
from tinyman.v1.client import TinymanMainnetClient, TinymanClient
from algo.blockchain.base import DataScraper
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
from typing import Callable, Optional
from abc import ABC, abstractmethod

pd.options.mode.chained_assignment = None  # default='warn'


class DataCacher(ABC):
    def __init__(self, universe_cache_name: str,
                 cache_basedir: str,
                 client: TinymanClient,
                 date_min: datetime.datetime,
                 date_max: Optional[datetime.datetime]):

        self.client = client
        self.pools = [(x.asset1_id, x.asset2_id) for x in Universe.from_cache(universe_cache_name).pools]
        self.date_min = date_min
        self.date_max = date_max
        for date in date_min, date_max:
            if date is not None:
                assert date == datetime.datetime(year=date.year, month=date.month, day=date.day), \
                    f"date_min, date_max = {date_min}, {date_max} must be dates without hours, minutes etc."
        self.cache_basedir = cache_basedir

    @abstractmethod
    def make_scraper(self, asset1_id:int, asset2_id:int):
        pass

    def cache(self, cache_name: str):
        basedir = os.path.join(self.cache_basedir, cache_name)
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

        if self.date_max is None:
            utcnow = datetime.datetime.utcnow()
            date_max = utcnow.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            date_max = self.date_max

        date_min = self.date_min
        while date_min < date_max:
            if date_min not in existing_dates:
                break
            date_min = date_min + datetime.timedelta(days=1)

        if date_min == date_max:
            print(f'Skipping assets {assets[0], assets[1]} because all data is present in the cache')
            return
        else:
            print(f'Found minimum date to scrape for assets {assets[0], assets[1]} = {date_min}')

        scraper = self.make_scraper(assets[0], assets[1])
        df = generator_to_df(scraper.scrape(num_queries=None,
                                            timestamp_min=datetime_to_int(date_min),
                                            before_time=date_max)
                             )
        if df.empty:
            return

        def cache_if_new(daydf: pd.DataFrame, date):
            fname = file_name(date)
            if os.path.exists(fname):
                pass
            else:
                table = pa.Table.from_pandas(daydf)
                pq.write_table(table, fname)

        dates = df['time'].dt.date

        df['time'] = df['time'].view(dtype=np.int64) // 1000000000

        # TODO check this is correct (we do not exclude the current day because we have filtered it in the query
        df.groupby(dates).apply(lambda x: cache_if_new(x, x.name))


