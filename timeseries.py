from datastore import DataStore, AssetData
import pandas as pd


def compute_moving_average(df: pd.Series, interval: str = "3h"):
    """Compute the moving average from processed_price_data"""
    return df.resample(interval).mean().fillna(0).rolling(window=3, min_periods=1).mean()


def make_df_single(asset_id: int, data: AssetData):
    subdf = pd.DataFrame(data=data.slow.price_history)
    subdf['MA_2h'] = compute_moving_average(subdf['price'], "2h")
    subdf['MA_15min'] = compute_moving_average(subdf['price'], "15min")
    subdf['asset_id'] = asset_id
    return subdf


def make_df(ds: DataStore) -> pd.DataFrame:
    return pd.concat([make_df_single(asset_id, data) for asset_id, data in ds.items()])
