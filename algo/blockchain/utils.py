import datetime
import glob
import re
import pandas as pd
from pathlib import Path
from definitions import ROOT_DIR
from algo.universe.universe import SimpleUniverse
from typing import Optional, Callable


def datetime_to_int(t: datetime.datetime):
    return int(t.timestamp())


def int_to_datetime(t: int):
    return datetime.datetime.fromtimestamp(t)


def int_to_rfc3339(t: int):
    return datetime.datetime.fromtimestamp(t).isoformat() + 'Z'


def int_to_tzaware_utc_datetime(t: int):
    return datetime.datetime.utcfromtimestamp(t).replace(tzinfo=datetime.timezone.utc)


def generator_to_df(gen, time_columns=('time',)):
    df = pd.DataFrame(gen)
    if df.empty:
        print("DataFrame is empty")
    else:
        for col in time_columns:
            df[col] = pd.to_datetime(df[col], unit='s', utc=True)
    return df


def load_from_cache(pattern, filter_pair: Optional[Callable]):
    def gen_data():
        for base_dir in glob.glob(pattern):
            name = Path(base_dir).name

            if re.match('[0-9]+_[0-9]+$', name):
                a0, a1 = tuple(int(x) for x in name.split('_'))

                if filter_pair is None or filter_pair(a0, a1):
                    df = pd.read_parquet(base_dir)
                    if not df.empty:
                        df = df.sort_values(by='time')
                        df['asset1'] = a0
                        df['asset2'] = a1
                    yield df

    return pd.concat(gen_data())


def algo_nousd_filter(a1, a2):
    exclude = [31566704,  # USDC
               312769,  # USDt
               567485181,  # LoudDefi
               ]

    # Select only pairs with Algo
    if a1 in exclude or a2 in exclude \
            or (a1 != 0 and a2 != 0):
        return False
    return True


def make_filter_from_universe(universe: SimpleUniverse):
    def filter_pair(a1, a2):
        return tuple(sorted([a1, a2])) in [tuple(sorted([x.asset1_id, x.asset2_id])) for x in universe.pools]
    return filter_pair


def load_algo_pools(cache_name: str, data_type: str, filter_pair: Optional[Callable]):
    assert data_type in ['prices', 'volumes']

    pattern = f'{ROOT_DIR}/caches/{data_type}/{cache_name}/*'

    return load_from_cache(pattern, filter_pair)


def join_caches_with_priority(caches: list[str], data_type: str, filter_pair: Optional[Callable]):
    data = []
    for cache_priority, cachename in enumerate(caches):
        subdf = load_algo_pools(cachename, data_type, filter_pair=filter_pair)
        subdf['cache_priority'] = cache_priority
        data.append(subdf)
    df = pd.concat(data)
    time_maxes = df.groupby(['asset1', 'cache_priority'])['time'].max()

    filt_idx = pd.Series(True, index=df.index)

    for x, time_max in time_maxes.items():
        aid, priority = x[0], x[1]
        filt_idx &= (df['cache_priority'] <= priority) | (df['asset1'] != aid) | (df['time'] > time_max)

    return df[filt_idx].drop(columns='cache_priority').sort_values(by='time')
