import logging
import pandas as pd
import datetime
import numpy as np
from matplotlib import pyplot as plt
from algo.blockchain.utils import load_algo_pools
from algo.strategy.analytics import process_market_df, make_weights
from typing import Optional
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from algo.signals.constants import ASSET_INDEX_NAME, TIME_INDEX_NAME
from algo.signals.responses import LookaheadResponse, ComputedLookaheadResponse
from algo.universe.universe import SimpleUniverse


def any_axis_1(x):
    if (isinstance(x, np.ndarray) and x.ndim == 2) or isinstance(x, pd.DataFrame):
        return x.any(axis=1)
    else:
        return x


def not_nan_mask(*vecs):
    return ~np.any([any_axis_1(np.isnan(x)) for x in vecs], axis=0)


class AnalysisDataStore:

    def __init__(self, price_cache: str, volume_cache: str, universe: SimpleUniverse, filter_liq: Optional[float]):

        self.logger = logging.getLogger(__name__)
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

        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools), "Need to provide a Universe with only Algo pools"

        df = df[df.index.get_level_values(ASSET_INDEX_NAME).isin(asset_ids)]

        df_ids = df.index.get_level_values(ASSET_INDEX_NAME).unique()
        missing_ids = [asset_id for asset_id in asset_ids if asset_id not in df_ids]
        if missing_ids:
            self.logger.error(f"asset ids {missing_ids} are missing from the dataframe generated from the cache")

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
