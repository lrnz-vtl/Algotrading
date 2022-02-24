import pandas as pd
import datetime
import numpy as np
from scipy.stats.mstats import winsorize
from abc import ABC, abstractmethod
from dataclasses import dataclass
from algo.signals.constants import ASSET_INDEX_NAME, TIME_INDEX_NAME
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pydantic import BaseModel

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

    def __init__(self, minutes_forward: int, start_minutes_forward: int = 0):
        assert minutes_forward % 5 == 0
        assert start_minutes_forward % 5 == 0
        assert start_minutes_forward < minutes_forward
        self.minutes_forward = minutes_forward
        self.start_minutes_forward = start_minutes_forward

    @property
    def lookahead_time(self) -> datetime.timedelta:
        return datetime.timedelta(minutes=self.minutes_forward)

    def _call(self, price: pd.Series) -> pd.Series:
        price = price.rename('price')

        time_forward = price.copy()
        time_forward.index = time_forward.index.set_levels(
            time_forward.index.levels[1] - datetime.timedelta(minutes=self.minutes_forward), TIME_INDEX_NAME)

        start_time_forward = price.copy()
        start_time_forward.index = start_time_forward.index.set_levels(
            start_time_forward.index.levels[1] - datetime.timedelta(minutes=self.start_minutes_forward), TIME_INDEX_NAME)

        prices = price.to_frame().join(time_forward, rsuffix='_forward').join(start_time_forward, rsuffix='_start')
        resp = (prices['price_forward'] - prices['price_start']) / prices['price_start']

        return resp


class WinsorizeResponse:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def fit(self, X, y, **kwargs):
        resp = y.copy()
        mask = ~np.isnan(resp)
        resp[mask] = winsorize(resp[mask], limits=(0.05, 0.05))
        self.pipeline.fit(X, resp, **kwargs)
        return self

    def fit_transform(self, X, y, **kwargs):
        self.pipeline = self.fit(X, y, **kwargs)
        return self.transform(X)

    def fit_predict(self, X, y, **kwargs):
        self.pipeline = self.fit(X, y, **kwargs)
        return self.predict(X)

    def __getattr__(self, attr):
        if attr == 'fit':
            return self.fit
        elif attr == 'fit_predict':
            return self.fit_predict
        elif attr == 'fit_transform':
            return self.fit_transform
        else:
            return getattr(self.pipeline, attr)
