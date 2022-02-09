import logging
import aiohttp
from algo.universe.universe import SmallUniverse
from tinyman.v1.client import TinymanClient
from algo.blockchain.utils import datetime_to_int, generator_to_df
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import json
import pandas as pd
import datetime
from datetime import timezone
from typing import Optional, Iterable, AsyncGenerator
from abc import ABC, abstractmethod
import asyncio

pd.options.mode.chained_assignment = None  # default='warn'


class DateScheduler:
    def __init__(self,
                 date_min: datetime.datetime,
                 date_max: Optional[datetime.datetime]):

        for date in date_min, date_max:
            if date is not None:
                assert date == datetime.datetime(year=date.year, month=date.month, day=date.day), \
                    f"date_min, date_max = {date_min}, {date_max} must be dates without hours, minutes etc."

        if date_max is None:
            utcnow = datetime.datetime.utcnow()
            self.date_max = utcnow.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            self.date_max = date_max

        self.date_min = date_min

    def get_dates_to_fetch(self, existing_dates: Iterable[datetime.date]) -> set[datetime.datetime]:

        needed_dates = set()
        date = self.date_min
        while date < self.date_max:
            if date.date() not in existing_dates:
                needed_dates.add(date)
            date = date + datetime.timedelta(days=1)

        return needed_dates


def get_existing_dates(cache_dir: str):
    try:
        with open(f'{cache_dir}.json') as json_file:
            return eval(json.load(json_file))
    except FileNotFoundError:
        return set()


def add_fetched_date(cache_dir: str, date):
    existing_dates = get_existing_dates(cache_dir)
    with open(f'{cache_dir}.json', 'w') as json_file:
        existing_dates.add(date)
        json.dump(existing_dates, json_file, default=str)


async def groupby_days(gen: AsyncGenerator):
    prev_date = None
    prev_data = []
    async for x in gen:
        date = datetime.datetime.fromtimestamp(x.time, timezone.utc).date()
        if prev_date is not None and date != prev_date and prev_data:
            yield prev_data
            prev_data = []
        prev_date = date
        prev_data.append(x)

    if prev_data:
        yield prev_data


class DataCacher(ABC):
    def __init__(self, cache_file: str,
                 cache_basedir: str,
                 client: TinymanClient,
                 date_min: datetime.datetime,
                 date_max: Optional[datetime.datetime]):

        self.client = client
        self.pools = [(x.asset1_id, x.asset2_id) for x in SmallUniverse.from_cache(cache_file).pools]

        self.dateScheduler = DateScheduler(date_min, date_max)

        self.cache_basedir = cache_basedir
        self.logger = logging.getLogger('DataCacher')

    @abstractmethod
    def make_scraper(self, asset1_id: int, asset2_id: int):
        pass

    def cache(self, cache_name: str):
        basedir = os.path.join(self.cache_basedir, cache_name)
        os.makedirs(basedir, exist_ok=True)

        async def main():
            async with aiohttp.ClientSession() as session:
                await asyncio.gather(*[self._cache_pool(session, assets, basedir) for assets in self.pools])

        asyncio.run(main())

    async def _cache_pool(self, session, assets, basedir):
        assets = list(sorted(assets, reverse=True))
        cache_dir = os.path.join(basedir, "_".join([str(x) for x in assets]))
        os.makedirs(cache_dir, exist_ok=True)

        def file_name(date):
            return os.path.join(cache_dir, f'{date}.parquet')

        existing_dates = get_existing_dates(cache_dir)

        dates_to_fetch = self.dateScheduler.get_dates_to_fetch(existing_dates)
        if len(dates_to_fetch) == 0:
            self.logger.info(f'Skipping assets {assets[0], assets[1]} because all data is present in the cache')
            return

        date_min = min(dates_to_fetch)
        date_max = max(dates_to_fetch) + datetime.timedelta(days=1)

        self.logger.info(
            f'Found min,max dates to scrape for assets {assets[0], assets[1]} = {date_min}, {date_max}')

        scraper = self.make_scraper(assets[0], assets[1])
        if scraper is None:
            self.logger.warning(f'Pool for assets {assets[0], assets[1]} does not exist')
            return

        def cache_day_df(daydf: pd.DataFrame, date):
            fname = file_name(date)
            table = pa.Table.from_pandas(daydf)
            pq.write_table(table, fname)

        async for data in groupby_days(scraper.scrape(session=session,
                                                      num_queries=None,
                                                      timestamp_min=datetime_to_int(date_min),
                                                      before_time=date_max)):
            df = generator_to_df(data)
            dates = df['time'].dt.date.unique()
            df['time'] = df['time'].view(dtype=np.int64) // 1000000000
            assert len(dates) == 1, f"{dates}"
            date = dates[0]
            cache_day_df(df, date)
            add_fetched_date(cache_dir, date)
            self.logger.info(f'Cached date {date} for assets {assets}')
