from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np
import pandas as pd
import sklearn
from algo.cpp.cseries import shift_forward, compute_ema
from sklearn.linear_model import Ridge

from algo.binance.compute_betas import BetaStore

ms_in_hour = 10 ** 3 * 60 * 60
max_lag_hours = 48


@dataclass
class FitResults:
    r2_score: float
    ypred: pd.Series
    ytrue: pd.Series


@dataclass
class ModelOptions:
    get_lm: Callable
    residualise: bool
    transform_fit_target: Optional[Callable] = None
    transform_model_after_fit: Optional[Callable] = None


class Fitter:
    def __init__(self, feature_df: pd.DataFrame):
        self.feature_df = feature_df
        max_lag_ms = ms_in_hour * max_lag_hours

        cutoff_time = ((feature_df.index.max() - feature_df.index.min()) * 0.66) + feature_df.index.min()
        self.train_idx = feature_df.index < cutoff_time
        self.test_idx = feature_df.index > cutoff_time + max_lag_ms

    def fit_to_target(self, target: pd.Series,
                      opt: ModelOptions,
                      bs: Optional[BetaStore]
                      ) -> FitResults:
        assert (target.index == self.feature_df.index).all()

        if bs is not None:
            beta = bs.compute_beta(target[self.train_idx])
            target = bs.residualise(beta, target)

        y = target[self.train_idx]

        if opt.transform_fit_target:
            y = opt.transform_fit_target(y)

        lm = opt.get_lm()
        lm.fit(self.feature_df[self.train_idx], y)

        if opt.transform_model_after_fit:
            lm = opt.transform_model_after_fit(lm)

        target_test = target[self.test_idx]
        pred = pd.Series(lm.predict(self.feature_df[self.test_idx]), index=target_test.index)
        r2 = sklearn.metrics.r2_score(target_test, pred)

        return FitResults(ypred=pred.rename('ypred'), ytrue=target_test.rename('ytrue'), r2_score=r2)


def emas_from_price(price_ts: pd.Series, decays_hours: list[float]) -> pd.DataFrame:
    fts = []

    for dh in decays_hours:
        dms = ms_in_hour * dh
        ema = compute_ema(price_ts.index, price_ts.values, dms)
        ft = np.log(ema) - np.log(price_ts)
        fts.append(pd.Series(ft, index=price_ts.index))

    return pd.concat(fts, axis=1)


class ProductDataStore:

    def __init__(self, price_ts: pd.Series,
                 decay_hours: list[int],
                 ):
        self.price_ts = price_ts
        features = emas_from_price(price_ts, decay_hours)

        self.fitter = Fitter(features)

    def compute_forward_price(self, forward_hour: int):
        fms = ms_in_hour * forward_hour
        return shift_forward(self.price_ts.index, self.price_ts.values, fms)

    def evaluate(self, target: pd.Series, opt: ModelOptions, bs: Optional[BetaStore]) -> FitResults:
        return self.fitter.fit_to_target(target, opt, bs)


class UniverseDataStore:
    def __init__(self, price_ts: pd.Series, decay_hours: list):
        assert len(price_ts.index.names) == 2
        pairs = price_ts.index.get_level_values('pair').unique()

        self.pds = {}

        for pair in pairs:
            ds = ProductDataStore(price_ts.loc[pair], decay_hours)
            self.pds[pair] = ds

    def evaluate(self, forward_hour: int, opt: ModelOptions) -> pd.DataFrame:

        targets = {}

        for pair, ds in self.pds.items():
            forward_price = ds.compute_forward_price(forward_hour=forward_hour)
            targets[pair] = np.log(forward_price) - np.log(ds.price_ts)

        bs = None
        if opt.residualise:
            target_df = pd.DataFrame(targets)
            bs = BetaStore(target_df)

        dfs = []

        for pair, ds in self.pds.items():
            res = ds.evaluate(targets[pair], opt, bs)

            subdf = pd.concat([res.ypred, res.ytrue], axis=1)
            subdf['pair'] = pair
            dfs.append(subdf)

        return pd.concat(dfs, axis=0)
