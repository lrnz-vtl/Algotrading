import logging
import pandas as pd
import datetime
import numpy as np
from algo.universe.assets import get_asset_name, get_decimals
from typing import Optional, Union


def make_algo_pricevolume(df):
    assert np.all(df['asset2'] == 0)

    def foo(subdf, asset_id):
        decimals = get_decimals(asset_id)
        subdf['algo_price'] = subdf['asset2_reserves'] / df['asset1_reserves'] * 10 ** (decimals - 6)
        subdf['algo_reserves'] = subdf['asset2_reserves'] / (10 ** 6)
        if 'asset2_amount' in subdf.columns:
            subdf['algo_volume'] = subdf['asset2_amount'] / (10 ** 6)

        return subdf

    df = df.groupby('asset1').apply(lambda x: foo(x, x.name))
    volcols = ['algo_volume', 'asset1_amount', 'asset2_amount']
    df[volcols] = df[volcols].fillna(0)
    return df


def timestamp_to_5min(time_col: pd.Series):
    # We do not want lookahead in the data, so each 5 minute slice should contain the data for the past, not the future
    time_5min = ((time_col // 300) + ((time_col % 300) > 0).astype(int)) * 300
    return pd.to_datetime(time_5min, unit='s', utc=True)


def ffill_cols(df: pd.DataFrame, cols: list[str], minutes_limit: Union[int, str], all_times=None):
    if all_times is None:
        all_times = []
        delta = df['time_5min'].max() - df['time_5min'].min()
        for i in range(int(delta.total_seconds() / (5 * 60))):
            all_times.append(df['time_5min'].min() + i * datetime.timedelta(seconds=5 * 60))

        all_times = pd.Series(all_times).rename('time_5min')
        assert len(all_times) == len(set(all_times))

    df = df.merge(all_times, on='time_5min', how='outer')
    df = df.sort_values(by='time_5min')

    df['time_5min_ffilled'] = df['time_5min'].fillna(method='ffill')

    if isinstance(minutes_limit, int):
        assert (minutes_limit % 5 == 0)
        timelimit_idx = ((df['time_5min'] - df['time_5min_ffilled']) <= datetime.timedelta(minutes=minutes_limit))
    elif minutes_limit == 'all':
        timelimit_idx = pd.Series(True, index=df.index)
    else:
        raise ValueError

    for col in cols:
        fill_idx = timelimit_idx & df[col].isna()
        col_ffilled = df[col].fillna(method='ffill')
        df.loc[fill_idx, col] = col_ffilled[fill_idx]

    return df


def ffill_prices(df: pd.DataFrame, minutes_limit: Union[int, str]):
    cols = ['asset1_reserves', 'asset2_reserves']

    assert ~df[cols].isna().any().any()

    ret: pd.DataFrame = df.groupby('asset1').apply(
        lambda x: ffill_cols(x.drop(columns=['asset1']), cols, minutes_limit)).reset_index()
    ret = ret.dropna(subset=cols)
    ret['asset2'] = 0
    return ret


def process_market_df(price_df: pd.DataFrame, volume_df: Optional[pd.DataFrame],
                      ffill_price_minutes: Optional[Union[int, str]],
                      price_agg_fun='mean',
                      merge_how='left',
                      ):
    logger = logging.getLogger(__name__)

    price_df['time_5min'] = timestamp_to_5min(price_df['time'])

    keys = ['time_5min', 'asset1', 'asset2']

    price_cols = ['asset1_reserves', 'asset2_reserves']
    price_df = price_df[price_cols + keys].groupby(keys).agg(price_agg_fun).reset_index()

    if ffill_price_minutes:
        in_shape = price_df.shape[0]
        price_df = ffill_prices(price_df, ffill_price_minutes)
        logger.info(f'Forward filled prices, shape ({in_shape}) -> ({price_df.shape[0]})')

    if volume_df is not None:
        volume_df['time_5min'] = timestamp_to_5min(volume_df['time'])
        volume_cols = ['asset1_amount', 'asset2_amount']
        for col in volume_cols:
            volume_df[col] = abs(volume_df[col])
        volume_df = volume_df[volume_cols + keys].groupby(keys).agg('sum').reset_index()

        df = price_df.merge(volume_df, how=merge_how, on=keys)
    else:
        df = price_df

    assert np.all(df['asset2'] == 0)
    return make_algo_pricevolume(df)
