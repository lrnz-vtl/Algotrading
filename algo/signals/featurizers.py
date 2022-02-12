import pandas as pd
from ts_tools_algo.features import generate_over_series, ema_provider
from typing import Callable
from abc import ABC, abstractmethod
from algo.signals.constants import ASSET_INDEX_NAME, TIME_INDEX_NAME


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
        ma = pd.Series(generate_over_series(ts, ema_provider(self.minutes * 60)), index=ts.index)
        return ((ts - ma) / ts).rename(self.name)


def concat_featurizers(fts: list[SingleASAFeaturizer]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    names = [ft.name for ft in fts]
    assert len(names) == len(set(names))

    def foo(df: pd.DataFrame):
        return pd.concat([ft(df) for ft in fts], axis=1)

    return foo
