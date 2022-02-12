import pandas as pd
import datetime
import numpy as np
from algo.universe.assets import get_asset_name, get_decimals


def make_algo_pricevolume(df):
    assert np.all(df['asset2'] == 0)

    def foo(subdf, asset_id):
        decimals = get_decimals(asset_id)
        subdf['algo_price'] = subdf['asset2_reserves'] / df['asset1_reserves'] * 10 ** (decimals - 6)
        subdf['algo_volume'] = subdf['asset2_amount'] / (10 ** 6)
        subdf['algo_reserves'] = subdf['asset2_reserves'] / (10 ** 6)

        return subdf

    return df.groupby('asset1').apply(lambda x: foo(x, x.name))


def make_weights(df: pd.DataFrame):
    return (df.groupby(['asset1', 'date'])['asset2_reserves'].agg('mean') / (10 ** 10)).rename('weight')


def timestamp_to_5min(time_col: pd.Series):
    # We do not want lookahead in the data, so each 5 minute slice should contain the data for the past, not the future
    time_5min = ((time_col // 300) + ((time_col % 300) > 0).astype(int)) * 300
    return pd.to_datetime(time_5min, unit='s', utc=True)


def process_market_df(price_df: pd.DataFrame, volume_df: pd.DataFrame):
    price_df['time_5min'] = timestamp_to_5min(price_df['time'])
    volume_df['time_5min'] = timestamp_to_5min(volume_df['time'])

    keys = ['time_5min', 'asset1', 'asset2']

    price_cols = ['asset1_reserves', 'asset2_reserves']
    price_df = price_df[price_cols + keys].groupby(keys).agg('mean').reset_index()

    volume_cols = ['asset1_amount', 'asset2_amount']
    for col in volume_cols:
        volume_df[col] = abs(volume_df[col])
    volume_df = volume_df[volume_cols + keys].groupby(keys).agg('sum').reset_index()

    df = price_df.merge(volume_df, how='left', on=keys)
    df['date'] = df['time_5min'].dt.date

    assert np.all(df['asset2'] == 0)
    df = make_algo_pricevolume(df)

    return df
