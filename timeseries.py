from datastore import DataStore, AssetData
import pandas as pd
import numpy as np

# from numba import jit

class ExpAverages:

    def __init__(self, ts: pd.Series, T: float):
        """ T is the time scale (expressed in seconds) """

        ema = np.empty(shape=ts.shape[0])
        emv = np.empty(shape=ts.shape[0])
        ema[:] = np.NaN
        emv[:] = np.NaN

        t0 = pd.NaT
        x0 = np.nan
        # TODO would be nice to wrap this in numba.jit, the import fails for me
        for i, (t, x) in enumerate(ts.iteritems()):
            if x is not np.nan:
                if x0 is np.nan:
                    ema[i] = x
                    emv[i] = 0
                else:
                    alpha = 1 - np.exp(-(t - t0).seconds / T)
                    ema[i] = alpha * x + (1 - alpha) * ema[i - 1]
                    emv[i] = (1 - alpha) * emv[i - 1] + alpha * (x - ema[i - 1]) ** 2
                t0 = t
                x0 = x

        self.ema = ema
        self.emv = emv


def exp_average(ts, T):
    ea = ExpAverages(ts, T)
    return ea.ema


def compute_moving_average(ts: pd.Series, interval: str = "3h"):
    """Compute the moving average from processed_price_data"""
    return ts.resample(interval).mean().fillna(0).rolling(window=3, min_periods=1).mean()


def make_asset_features(subdf, colname='price'):
    assert len(subdf['asset_id'].unique()) == 1

    subdf['ema_2h'] = exp_average(subdf[colname], 2 * 60 * 60)
    subdf['ema_1h'] = exp_average(subdf[colname], 1 * 60 * 60)
    subdf['ema_30m'] = exp_average(subdf[colname], 30 * 60)

    subdf['MA_2h'] = compute_moving_average(subdf[colname], "2h")
    subdf['MA_15min'] = compute_moving_average(subdf[colname], "15min")

    return subdf


def make_df(ds: DataStore) -> pd.DataFrame:
    def make_single(asset_id: int, data: AssetData):
        df = pd.DataFrame(data=data.slow.price_history)
        df['asset_id'] = asset_id
        return df

    return pd.concat([make_single(asset_id, data) for asset_id, data in ds.items()])
