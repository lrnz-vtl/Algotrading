import logging

import pandas as pd
import datetime
import numpy as np
from matplotlib import pyplot as plt
from algo.blockchain.utils import load_algo_pools
from algo.strategy.analytics import process_market_df, make_weights
from ts_tools_algo.features import exp_average
from typing import Optional
from typing import Callable
from scipy.stats.mstats import winsorize
from sklearn.linear_model import LinearRegression
from abc import ABC, abstractmethod
from dataclasses import dataclass
from sklearn.metrics import r2_score

ASSET_INDEX_NAME = 'asset1'
TIME_INDEX_NAME = 'time_5min'


class SingleASAFeaturizer(ABC):

    @abstractmethod
    def _call(self, df: pd.DataFrame) -> pd.Series:
        pass

    @property
    @abstractmethod
    def name(self):
        pass

    def __call__(self, df: pd.DataFrame) -> pd.Series:
        assert df.index.names == [TIME_INDEX_NAME]
        return self._call(df)


class MAPriceFeaturizer(SingleASAFeaturizer):
    def __init__(self, minutes: int, price_col: str = 'algo_price'):
        self.minutes = minutes
        self.price_col = price_col

    @property
    def name(self):
        return f'ma_{self.minutes}'

    def _call(self, df: pd.DataFrame):
        ts = df[self.price_col]
        ma = pd.Series(exp_average(ts, self.minutes * 60), index=ts.index)
        return ((ts - ma) / ts).rename(self.name)


def concat_featurizers(fts: list[SingleASAFeaturizer]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    names = [ft.name for ft in fts]
    assert len(names) == len(set(names))

    def foo(df: pd.DataFrame):
        return pd.concat([ft(df) for ft in fts], axis=1)

    return foo


@dataclass
class ComputedLookaheadResponse:
    ts: pd.Series
    lookahead_time: datetime.timedelta


class LookaheadResponse(ABC):

    @property
    @abstractmethod
    def lookahead_time(self) -> datetime.timedelta:
        pass

    @abstractmethod
    def _call(self, price: pd.Series) -> pd.Series:
        pass

    def __call__(self, price: pd.Series) -> ComputedLookaheadResponse:
        assert price.index.names == [ASSET_INDEX_NAME, TIME_INDEX_NAME]
        return ComputedLookaheadResponse(self._call(price), self.lookahead_time)


class SimpleResponse(LookaheadResponse):

    def __init__(self, minutes_forward: int):
        assert minutes_forward % 5 == 0
        self.minutes_forward = minutes_forward

    @property
    def lookahead_time(self) -> datetime.timedelta:
        return datetime.timedelta(minutes=self.minutes_forward)

    def _call(self, price: pd.Series) -> pd.Series:
        price = price.rename('price')
        time_forward = price.copy()
        time_forward.index = time_forward.index.set_levels(
            time_forward.index.levels[1] - datetime.timedelta(minutes=self.minutes_forward), TIME_INDEX_NAME)

        prices = price.to_frame().join(time_forward, rsuffix='_forward')
        resp = (prices['price_forward'] - prices['price']) / prices['price']

        resp_winsor = resp.copy()
        mask = ~np.isnan(resp_winsor)
        resp_winsor[mask] = winsorize(resp_winsor[mask], limits=(0.05, 0.05))
        return resp_winsor


def any_axis_1(x):
    if (isinstance(x, np.ndarray) and x.ndim == 2) or isinstance(x, pd.DataFrame):
        return x.any(axis=1)
    else:
        return x


def not_nan_mask(*vecs):
    return ~np.any([any_axis_1(np.isnan(x)) for x in vecs], axis=0)


class AnalysisDataStore:

    def __init__(self, price_cache: str, volume_cache: str,
                 filter_liq: Optional[float],
                 filter_asset: set[int]
                 ):

        self.logger = logging.getLogger('AnalysisDataStore')
        self.logger.setLevel(level=logging.INFO)

        dfp = load_algo_pools(price_cache, 'prices')
        dfv = load_algo_pools(volume_cache, 'volumes')

        df = process_market_df(dfp, dfv)
        df = df.set_index([ASSET_INDEX_NAME, TIME_INDEX_NAME]).sort_index()
        assert np.all(df['asset2'] == 0)
        df = df.drop(columns=['asset2'])

        # Filter on liquidity
        if filter_liq is not None:
            df = df[df['algo_reserves'] > filter_liq]

        df = df[~df.index.get_level_values(ASSET_INDEX_NAME).isin(filter_asset)]

        ws = make_weights(df)
        weights = df[['date']].merge(ws, on=[ASSET_INDEX_NAME, 'date'], how='left')
        weights.index = df.index
        weights = weights['weight']

        self.df = df
        self.weights = weights

        assert self.df.index.names == [ASSET_INDEX_NAME, TIME_INDEX_NAME]

    def make_response(self, response_maker: LookaheadResponse) -> ComputedLookaheadResponse:
        return response_maker(self.df['algo_price'])

    def bootstrap_arrays(self, n: int, *vecs):
        assets = self.df.index.get_level_values(0).unique()
        for i in range(n):
            boot_assets = np.random.choice(assets, len(assets))
            yield tuple(
                np.concatenate([vec[self.df.index.get_loc(boot_asset)] for boot_asset in boot_assets]) for vec in vecs)

    def make_asset_features(self, featurizer):
        return self.df.groupby([ASSET_INDEX_NAME]).apply(lambda x: featurizer(x.droplevel(0)))

    def eval_feature(self, feature: pd.Series, response: ComputedLookaheadResponse):

        N = 100
        betas = np.zeros(N)

        for i, (x, y, w) in enumerate(self.bootstrap_arrays(N, feature, response.ts, self.weights)):
            mask = not_nan_mask(x, y, w)
            x, y, w = x[mask], y[mask], w[mask]
            betas[i] = LinearRegression().fit(x[:, None], y, w).coef_

        return betas

    def make_train_val_splits(self, lookahead_time: datetime.timedelta, val_frac=0.25):
        sorted_ts = sorted(self.df.index.get_level_values(TIME_INDEX_NAME))

        i = int(len(sorted_ts) * (1 - val_frac))
        first_val_ts = sorted_ts[i]
        upper_train_ts = first_val_ts - lookahead_time

        train_idx = self.df.index.get_level_values(TIME_INDEX_NAME) < upper_train_ts
        test_idx = self.df.index.get_level_values(TIME_INDEX_NAME) >= first_val_ts

        assert len(self.df) > train_idx.sum() + test_idx.sum()
        assert ~np.any(train_idx & test_idx)

        self.logger.info(f'Train, val size = {train_idx.sum()}, {test_idx.sum()}')

        return train_idx, test_idx

    def eval_model(self, model,
                   features: pd.DataFrame,
                   response: ComputedLookaheadResponse,
                   filter_nans: bool):

        X, y, w = features, response.ts, self.weights

        train_idx, test_idx = self.make_train_val_splits(response.lookahead_time)

        if filter_nans:
            mask = not_nan_mask(X, y, w)
            X_full, y_full, w_full = X[mask], y[mask], w[mask]
            train_idx = train_idx & mask
            test_idx = test_idx & mask
            self.logger.info(f'Train, val size after removing NaNs = {train_idx.sum()}, {test_idx.sum()}')
        else:
            X_full, y_full, w_full = X, y, w

        X_train, y_train, w_train = X[train_idx], y[train_idx], w[train_idx]
        X_test, y_test, w_test = X[test_idx], y[test_idx], w[test_idx]

        train_times = X_train.index.get_level_values(TIME_INDEX_NAME)
        test_times = X_test.index.get_level_values(TIME_INDEX_NAME)
        assert train_times.max() < test_times.min()
        self.logger.info(f'Max train time = {train_times.max()}, Min test time = {test_times.min()}')

        m = model.fit(X_train, y_train, sample_weight=w_train)

        y_pred = m.predict(X_test)
        oos_rsq = r2_score(y_test, y_pred, sample_weight=w_test)
        self.logger.info(f'OOS R^2 = {oos_rsq}')

        sig = m.predict(X_full)
        xxw = w_full * sig ** 2
        xyw = w_full * sig * y_full

        asset_ids = self.df.index.get_level_values(ASSET_INDEX_NAME).unique()
        cols = len(asset_ids)
        f, axs = plt.subplots(1, cols, figsize=(10 * cols, 4))

        for asset, ax in zip(asset_ids, axs):
            xxw.loc[asset].groupby(TIME_INDEX_NAME).sum().cumsum().plot(label='signal', ax=ax)
            xyw.loc[asset].groupby(TIME_INDEX_NAME).sum().cumsum().plot(label='response', ax=ax)
            ax.axvline(X_test.index.get_level_values(TIME_INDEX_NAME).min(), ls='--', color='k', label='test cutoff')
            ax.set_title(asset)
            ax.legend()
            ax.grid()

        plt.show()
