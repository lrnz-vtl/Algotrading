import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

data_base = Path('/home/lorenzo/microprice')
fname = 'binance-futures_book_snapshot_5_2020-09-01_BTCUSDT.csv'
fname_parquet = 'binance-futures_book_snapshot_5_2020-09-01_BTCUSDT.parquet'

# start = datetime.datetime(year=2023, month=1, day=2, hour=7, minute=14)
# end = datetime.datetime(year=2023, month=1, day=2, hour=7, minute=16)

# df = pd.read_csv(data_base / fname)
# df.to_parquet(data_base / fname_parquet)
df = pd.read_parquet(data_base / fname_parquet)

print(df.dtypes)


def process_df(df):
    # df.columns = ['trade Id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker', 'isBestMatch']
    df['time'] = pd.to_datetime(df['timestamp'], unit='us')
    df = df.set_index('time')
    # idx = ((df.index > start) & (df.index < end))
    # df = df[idx]
    return df


df = process_df(df)


def process_series(ts: np.array):
    i = 0
    j = 0
    print(ts)
    delta = datetime.timedelta(seconds=1)
    ret = ts.copy()
    for i in range(len(ts)):
        while j < len(ts) and ts[j] - ts[i] < delta:
            j += 1
        ret[i] = ts[j]
    return ret


mid = (df['asks[0].price'] + df['bids[0].price']) / 2.0

idx2 = process_series(mid.index.values)

print(df.head())

# df = process_df(df)
# df2 = process_df(df2)

# df['price'].plot()
