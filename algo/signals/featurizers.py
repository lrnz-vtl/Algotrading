import pandas as pd
from ts_tools_algo.features import generate_over_series, ema_provider
from typing import Callable
from abc import ABC, abstractmethod
from algo.signals.constants import ASSET_INDEX_NAME, TIME_INDEX_NAME
from ts_tools_algo.series import frac_diff
import numpy as np


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
        return self._call(df).rename(self.name)


class MAPriceFeaturizer(SingleASAFeaturizer):
    def __init__(self, minutes: int, price_col: str = 'algo_price'):
        self.minutes = minutes
        self.price_col = price_col

    @property
    def name(self):
        return f'ma_{self.minutes}'

    def _call(self, df: pd.DataFrame):
        ts = df[self.price_col]
        ma = pd.Series(generate_over_series(ts, ema_provider(self.minutes * 60)), index=ts.index)
        return (ts - ma) / ts


class FracDiffFeaturizer(SingleASAFeaturizer):
    def __init__(self, price_col: str = 'algo_price', d=0.4, thres=0.98):
        self.price_col = price_col
        self.d = d
        self.thres = thres

    @property
    def name(self):
        return f'frac_{self.d}'

    def _call(self, df: pd.DataFrame):
        ts = df[self.price_col]
        return frac_diff(ts, self.d, self.thres) / ts


class FracDiffEMA(SingleASAFeaturizer):
    def __init__(self, minutes: int, d=0.4, price_col: str = 'algo_price', thres=0.98):
        self.price_col = price_col
        self.d = d
        self.thres = thres
        self.minutes = minutes

    @property
    def name(self):
        return f'frac_{self.d}_ma{self.minutes}'

    def _call(self, df: pd.DataFrame):
        ts = df[self.price_col]
        fd = frac_diff(ts, self.d, self.thres).fillna(0)

        startidx = np.where(~fd.isna())[0][0]

        fd_ma = pd.Series(np.nan, index=fd.index)
        fd_ma[startidx:] = pd.Series(generate_over_series(fd[startidx:], ema_provider(self.minutes * 60)),
                                     index=fd.index)
        assert ~fd_ma.isna().any()
        return fd_ma / ts


def concat_featurizers(fts: list[SingleASAFeaturizer]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    names = [ft.name for ft in fts]
    assert len(names) == len(set(names))

    def foo(df: pd.DataFrame):
        return pd.concat([ft(df) for ft in fts], axis=1)

    return foo
