import datetime
import glob
import pandas as pd
from pathlib import Path
from definitions import ROOT_DIR


def datetime_to_int(t: datetime.datetime):
    return int(t.timestamp())


def generator_to_df(gen, time_columns=('time',)):
    df = pd.DataFrame(gen)
    if df.empty:
        print("DataFrame is empty")
    else:
        for col in time_columns:
            df[col] = pd.to_datetime(df[col], unit='s', utc=True)
    return df


def timestamp_to_5min(time_col: pd.Series):
    # We do not want lookahead in the data, so each 5 minute slice should contain the data for the past, not the future
    time_5min = ((time_col // 300) + ((time_col % 300) > 0).astype(int)) * 300
    return pd.to_datetime(time_5min, unit='s', utc=True)


def load_from_cache(pattern, filter_pair):
    def gen_data():
        for base_dir in glob.glob(pattern):
            a0, a1 = tuple(int(x) for x in Path(base_dir).name.split('_'))

            if filter_pair(a0, a1):
                df = pd.read_parquet(base_dir)
                if not df.empty:
                    df = df.sort_values(by='time')
                    df['asset1'] = a0
                    df['asset2'] = a1
                yield df

    df = pd.concat(gen_data())
    df['time_5min'] = timestamp_to_5min(df['time'])
    return df


def load_algo_pools(cache_name: str, data_type: str):

    assert data_type in ['prices', 'volumes']

    def filter_pair(a1, a2):
        exclude = [31566704,  # USDC
                   312769,  # USDt
                   567485181,  # LoudDefi
                   ]
        if a1 in exclude or a2 in exclude:
            return False
        return True

    # Select only pairs with Algo
    pattern = f'{ROOT_DIR}/caches/{data_type}/{cache_name}/*_0'

    return load_from_cache(pattern, filter_pair)
