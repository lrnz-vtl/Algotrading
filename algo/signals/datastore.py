import logging
import pandas as pd
import datetime
import numpy as np
from algo.dataloading.caching import join_caches_with_priority, make_filter_from_universe
from algo.strategy.analytics import process_market_df
from algo.signals.constants import ASSET_INDEX_NAME, TIME_INDEX_NAME
from algo.signals.weights import BaseWeightMaker
from algo.signals.responses import LookaheadResponse, ComputedLookaheadResponse
from algo.universe.universe import SimpleUniverse
from typing import Optional, Union
from algo.signals.evaluation import FittableDataStore
from abc import ABC, abstractmethod
from ts_tools_algo.series import rolling_min


class DataFilter(ABC):

    @abstractmethod
    def apply(self, df: pd.DataFrame):
        pass

    def __call__(self, df: pd.DataFrame):
        assert df.index.names == [ASSET_INDEX_NAME, TIME_INDEX_NAME]
        return df.groupby(ASSET_INDEX_NAME).apply(lambda x: self.apply(x.droplevel(ASSET_INDEX_NAME)))


class RollingLiquidityFilter(DataFilter):

    def __init__(self, period_days: int = 7, algo_reserves_floor=50000):
        self.period_days = period_days
        self.algo_reserves_floor = algo_reserves_floor

    def apply(self, df: pd.DataFrame):
        period = datetime.timedelta(days=self.period_days)
        rol_min_liq = rolling_min(df['asset2_reserves'], period)
        return (rol_min_liq / 10 ** 6) > self.algo_reserves_floor


def lag_market_data(df: pd.DataFrame, lag_seconds: int):
    df = df.copy()
    df['time'] = df['time'] + lag_seconds
    return df


def process_dfs(dfp: pd.DataFrame, dfv: Optional[pd.DataFrame], ffill_price_minutes):
    df = process_market_df(dfp, dfv, ffill_price_minutes)
    df = df.set_index([ASSET_INDEX_NAME, TIME_INDEX_NAME]).sort_index()
    assert np.all(df['asset2'] == 0)
    return df.drop(columns=['asset2', 'level_1'], errors='ignore')


class AnalysisDataStore:

    def __init__(self,
                 price_caches: list[str],
                 volume_caches: list[str],
                 universe: SimpleUniverse,
                 weight_maker: BaseWeightMaker,
                 ffill_price_minutes: Optional[Union[int, str]],
                 market_lag_seconds: int):

        self.features_lag_seconds = market_lag_seconds

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level=logging.INFO)

        asset_ids = [pool.asset1_id for pool in universe.pools]
        assert all(pool.asset2_id == 0 for pool in universe.pools), "Need to provide a Universe with only Algo pools"

        filter_ = make_filter_from_universe(universe)

        dfp = join_caches_with_priority(price_caches, 'prices', filter_)
        dfp_lagged = lag_market_data(dfp, self.features_lag_seconds)
        if volume_caches:
            dfv = join_caches_with_priority(volume_caches, 'volumes', filter_)
            dfv_lagged = lag_market_data(dfv, self.features_lag_seconds)
        else:
            dfv = None
            dfv_lagged = None

        df = process_dfs(dfp, dfv, ffill_price_minutes)
        df_lagged = process_dfs(dfp_lagged, dfv_lagged, ffill_price_minutes)

        df = df[df.index.get_level_values(ASSET_INDEX_NAME).isin(asset_ids)]
        df_lagged = df_lagged[df_lagged.index.get_level_values(ASSET_INDEX_NAME).isin(asset_ids)]

        idx = df.index.intersection(df_lagged.index)
        df = df.loc[idx]
        df_lagged = df_lagged.loc[idx]

        assert np.all(df.index == df_lagged.index)

        df_ids = df.index.get_level_values(ASSET_INDEX_NAME).unique()
        missing_ids = [asset_id for asset_id in asset_ids if asset_id not in df_ids]
        if missing_ids:
            self.logger.error(f"asset ids {missing_ids} are missing from the dataframe generated from the cache")

        self.df = df
        self.df_lagged = df_lagged
        self.weights = weight_maker(df)

        assert self.df.index.names == [ASSET_INDEX_NAME, TIME_INDEX_NAME]

    def make_response(self, response_maker: LookaheadResponse) -> ComputedLookaheadResponse:
        return response_maker(self.df['algo_price'])

    def make_asset_features(self, featurizer):
        return self.df_lagged.groupby([ASSET_INDEX_NAME]).apply(lambda x: featurizer(x.droplevel(0)))

    def make_fittable_data(self,
                           features: pd.DataFrame, response: ComputedLookaheadResponse,
                           trading_filters: list[DataFilter],
                           feature_filters: list[DataFilter],
                           filter_nan_responses: bool = False,
                           filter_nan_features: bool = False
                           ) -> FittableDataStore:

        assert np.all(response.ts.index == self.df.index)
        assert np.all(features.index == self.df.index)

        filt_idx = pd.Series(True, index=self.df.index)
        for filt in trading_filters:
            filt_idx = filt_idx & filt(self.df)
        for filt in feature_filters:
            filt_idx = filt_idx & filt(features)

        self.logger.info('Percentage of data retained after trading and features filters: '
                         f'{self.weights[filt_idx].sum() / self.weights.sum()}')

        if filter_nan_responses:
            self.logger.warning(
                'Filtering nan responses, this could potentially introduce bias or throw away too much data')
            filt_idx = filt_idx & (~response.ts.isna())

            self.logger.info(f'Percentage of data retained after filtering nan responses: '
                             f'{self.weights[filt_idx].sum() / self.weights.sum()}')

        if filter_nan_features:
            filt_idx = filt_idx & (~features.isna().any(axis=1))
            self.logger.info(f'Percentage of data retained after filtering nan features: '
                             f'{self.weights[filt_idx].sum() / self.weights.sum()}')

        prefilter_ids = set(self.weights.index.get_level_values(ASSET_INDEX_NAME).unique())
        postfilter_ids = set(self.weights[filt_idx].index.get_level_values(ASSET_INDEX_NAME).unique())
        if prefilter_ids != postfilter_ids:
            self.logger.warning(f'After filtering some ids were dropped! {prefilter_ids}, {postfilter_ids}')

        return FittableDataStore(features[filt_idx],
                                 ComputedLookaheadResponse(response.ts[filt_idx], response.lookahead_time),
                                 self.weights[filt_idx]
                                 )
