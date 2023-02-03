import logging
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Callable, Optional, Any
import numpy as np
import pandas as pd
from algo.cpp.cseries import shift_forward, compute_ema
from sklearn.base import BaseEstimator
from sklearn.linear_model._base import LinearModel
from algo.binance.compute_betas import BetaStore

ms_in_hour = (10 ** 3) * 60 * 60
max_lag_hours = 48

ms_in_month = ms_in_hour * 30 * 24
train_months = 8


class NotEnoughDataException(Exception):
    pass


@dataclass
class TrainTestData:
    ypred: pd.Series
    ytrue: pd.Series


@dataclass
class FitResults:
    train: TrainTestData
    test: TrainTestData
    fitted_model: LinearModel


@dataclass
class ModelOptions:
    get_lm: Callable[[], LinearModel]
    transform_fit_target: Optional[Callable] = None
    transform_model_after_fit: Optional[Callable] = None


def emas_diffs_from_price(price_ts: pd.Series, decays_hours: list[float]) -> pd.DataFrame:
    fts = []

    assert len(decays_hours) > 0

    def make_ema(dh):
        dms = ms_in_hour * dh
        return compute_ema(price_ts.index, price_ts.values, dms)

    logema0 = make_ema(decays_hours[0])
    for dh in decays_hours[1:]:
        ema = make_ema(dh)
        ft = np.log(ema) - logema0
        fts.append(pd.Series(ft, index=price_ts.index))

    return pd.concat(fts, axis=1)


def fit_eval_model(Xtrain,
                   Xtest,
                   ytrain,
                   ytest,
                   opt: ModelOptions):
    lm = opt.get_lm()

    if opt.transform_fit_target:
        ytrain = pd.Series(opt.transform_fit_target(ytrain), index=ytrain.index)

    lm.fit(Xtrain, ytrain)

    if opt.transform_model_after_fit:
        lm = opt.transform_model_after_fit(lm)

    ypred_train = pd.Series(lm.predict(Xtrain), index=ytrain.index)

    ypred_test = pd.Series(lm.predict(Xtest), index=ytest.index)

    return FitResults(
        fitted_model=lm,
        train=TrainTestData(ypred=ypred_train.rename('ypred'),
                            ytrue=ytrain.rename('ytrue')),
        test=TrainTestData(ypred=ypred_test.rename('ypred'),
                           ytrue=ytest.rename('ytrue')),
    )


class ProductDataStore:

    def __init__(self, price_ts: pd.Series, decay_hours: list[int]):
        self.logger = logging.getLogger(__name__)

        self.price_ts = price_ts
        self.feature_df = emas_diffs_from_price(price_ts, decay_hours)

        end_train_time = self.feature_df.index.min() + train_months * ms_in_month
        self.start_test_time = end_train_time + max_lag_hours * ms_in_hour

        self.train_idx = self.feature_df.index < end_train_time
        self.test_idx = self.feature_df.index > self.start_test_time

        if self.test_idx.sum() == 0:
            raise NotEnoughDataException()

    def make_target(self, forward_hour: int) -> tuple[pd.Series, pd.Series]:
        fms = ms_in_hour * forward_hour
        forward_price = shift_forward(self.price_ts.index, self.price_ts.values, fms)
        target = np.log(forward_price) - np.log(self.price_ts)

        return target.loc[self.train_idx], target.loc[self.test_idx]


@dataclass
class ResidOptions:
    market_pairs: set[str]
    hours_forward: int = 1


@dataclass
class UniverseFitOptions:
    demean: bool
    forward_hour: int
    target_scaler: Optional[Callable[[], Any]]
    global_model_options: Optional[ModelOptions]


@dataclass
class UniverseFitResults:
    train_targets: dict[str, pd.Series]
    test_targets: dict[str, pd.Series]
    vol_rescalings: Optional[dict[str, float]]
    res_global: Optional[FitResults]


class UniverseDataStore:
    def __init__(self, price_ts: pd.Series, decay_hours: list, resid_options: Optional[ResidOptions]):

        self.logger = logging.getLogger(__name__)

        assert len(price_ts.index.names) == 2
        pairs = price_ts.index.get_level_values('pair').unique()

        self.pds = {}
        self.pairs = []

        min_test_time = max(price_ts.index.get_level_values(1))

        mkt_features = []
        for pair in resid_options.market_pairs:
            assert pair in pairs
            mkt_features.append(emas_diffs_from_price(price_ts.loc[pair], decay_hours))
        if mkt_features:
            self.mkt_features = pd.concat(mkt_features, axis=1)
        else:
            self.mkt_features = None

        for pair in pairs:
            if pair in resid_options.market_pairs:
                continue
            try:
                ds = ProductDataStore(price_ts.loc[pair], decay_hours)
                min_test_time = min(min_test_time, ds.start_test_time)
            except NotEnoughDataException as e:
                self.logger.warning(f'Not enough data for {pair=}. Skipping.')
                continue

            self.pairs.append(pair)
            self.pds[pair] = ds

        self.bs = None
        if resid_options:
            self.bs = BetaStore(price_ts,
                                min_test_time=min_test_time,
                                hours_forward=resid_options.hours_forward)

    def fit_global(self, fit_options: UniverseFitOptions) -> UniverseFitResults:
        train_targets = {}
        test_targets = {}

        for pair, ds in self.pds.items():
            train_targets[pair], test_targets[pair] = ds.make_target(forward_hour=fit_options.forward_hour)

        if self.bs:
            for pair in self.pairs:
                beta = self.bs.compute_beta(train_targets[pair])
                train_targets[pair] = self.bs.residualise(beta, train_targets[pair])
                test_targets[pair] = self.bs.residualise(beta, test_targets[pair])

        if fit_options.demean:
            train_ret_df = pd.DataFrame(train_targets)
            test_ret_df = pd.DataFrame(test_targets)

            for pair in self.pairs:
                train_targets[pair] = train_targets[pair] - train_ret_df.mean(axis=1).loc[train_targets[pair].index]
                test_targets[pair] = test_targets[pair] - test_ret_df.mean(axis=1).loc[test_targets[pair].index]

            del train_ret_df
            del test_ret_df

        if fit_options.target_scaler is not None:
            vol_rescalings = {}
            for pair in self.pairs:
                vol_rescalings[pair] = fit_options.target_scaler().fit(train_targets[pair].values.reshape(-1, 1)).scale_[0]
                train_targets[pair] /= vol_rescalings[pair]
                test_targets[pair] /= vol_rescalings[pair]
        else:
            vol_rescalings = None

        if fit_options.global_model_options:
            train_target = pd.concat((train_targets[pair] for pair in self.pairs), axis=0)
            test_target = pd.concat((test_targets[pair] for pair in self.pairs), axis=0)

            features_train = pd.concat((self.pds[pair].feature_df[self.pds[pair].train_idx] for pair in self.pairs),
                                       axis=0)
            features_test = pd.concat((self.pds[pair].feature_df[self.pds[pair].test_idx] for pair in self.pairs),
                                      axis=0)

            if self.mkt_features is not None:
                features_train = pd.concat([features_train, self.mkt_features.loc[features_train.index].fillna(0)],
                                           axis=1)
                features_test = pd.concat([features_test, self.mkt_features.loc[features_test.index].fillna(0)], axis=1)

            res_global = fit_eval_model(features_train, features_test, train_target, test_target,
                                        fit_options.global_model_options)
            del features_train
            del features_test

        else:
            res_global = None

        return UniverseFitResults(train_targets=train_targets,
                                  test_targets=test_targets,
                                  res_global=res_global,
                                  vol_rescalings=vol_rescalings)

    def fit_products(self, ur: UniverseFitResults, model_options: ModelOptions, exclude_pairs: set[str] = None) -> dict[
        str, FitResults]:

        ress = {}

        for pair, ds in self.pds.items():
            if exclude_pairs is not None and pair in exclude_pairs:
                continue

            train_target = ur.train_targets[pair]
            test_target = ur.test_targets[pair]

            features_train = ds.feature_df[ds.train_idx]
            features_test = ds.feature_df[ds.test_idx]

            if self.mkt_features is not None:
                features_train = pd.concat([features_train, self.mkt_features.loc[features_train.index].fillna(0)],
                                           axis=1)
                features_test = pd.concat([features_test, self.mkt_features.loc[features_test.index].fillna(0)], axis=1)

            if ur.res_global:
                train_global_pred = ur.res_global.fitted_model.predict(features_train)
                test_global_pred = ur.res_global.fitted_model.predict(features_test)

                train_residual = train_target - train_global_pred
                test_residual = test_target - test_global_pred

                res = fit_eval_model(features_train,
                                     features_test,
                                     train_residual,
                                     test_residual,
                                     model_options)

                res.test.ypred = res.test.ypred + test_global_pred
                res.test.ytrue = test_target.rename('ytrue')
            else:
                res = fit_eval_model(features_train,
                                     features_test,
                                     train_target,
                                     test_target,
                                     model_options)

            if ur.vol_rescalings is not None:
                res.test.ypred *= ur.vol_rescalings[pair]
                res.test.ytrue *= ur.vol_rescalings[pair]

            assert (res.train.ypred.index == res.train.ytrue.index).all()
            assert (res.test.ypred.index == res.test.ytrue.index).all()

            ress[pair] = res

        return ress
