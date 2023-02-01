import datetime
import logging
import unittest
from dataclasses import dataclass
from typing import Callable, Optional

import numpy as np
import pandas as pd
from algo.cpp.cseries import shift_forward, compute_ema
from matplotlib import pyplot as plt
from sklearn.linear_model import Ridge

from algo.binance.coins import Universe, load_universe_candles
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


@dataclass
class ModelOptions:
    get_lm: Callable
    demean: bool
    transform_fit_target: Optional[Callable] = None
    transform_model_after_fit: Optional[Callable] = None


def emas_from_price(price_ts: pd.Series, decays_hours: list[float]) -> pd.DataFrame:
    fts = []

    for dh in decays_hours:
        dms = ms_in_hour * dh
        ema = compute_ema(price_ts.index, price_ts.values, dms)
        ft = np.log(ema) - np.log(price_ts)
        fts.append(pd.Series(ft, index=price_ts.index))

    return pd.concat(fts, axis=1)


def fit_eval_model(Xtrain, Xtest, ytrain, ytest, opt: ModelOptions):
    lm = opt.get_lm()

    if opt.transform_fit_target:
        ytrain = pd.Series(opt.transform_fit_target(ytrain), index=ytrain.index)

    lm.fit(Xtrain, ytrain)

    if opt.transform_model_after_fit:
        lm = opt.transform_model_after_fit(lm)

    ypred_train = pd.Series(lm.predict(Xtrain), index=ytrain.index)

    ypred_test = pd.Series(lm.predict(Xtest), index=ytest.index)

    return FitResults(
        train=TrainTestData(ypred=ypred_train.rename('ypred'),
                            ytrue=ytrain.rename('ytrue')),
        test=TrainTestData(ypred=ypred_test.rename('ypred'),
                           ytrue=ytest.rename('ytrue')),
    )


class ProductDataStore:

    def __init__(self, price_ts: pd.Series, decay_hours: list[int]):
        self.logger = logging.getLogger(__name__)

        self.price_ts = price_ts
        self.feature_df = emas_from_price(price_ts, decay_hours)

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

        test_target = target.loc[self.test_idx]

        return target.loc[self.train_idx], test_target


class UniverseDataStore:
    def __init__(self, price_ts: pd.Series, residualize: bool, decay_hours: list):

        self.logger = logging.getLogger(__name__)

        assert len(price_ts.index.names) == 2
        pairs = price_ts.index.get_level_values('pair').unique()

        self.pds = {}
        self.pairs = []

        min_test_time = max(price_ts.index.get_level_values(1))

        for pair in pairs:
            try:
                ds = ProductDataStore(price_ts.loc[pair], decay_hours)
                min_test_time = min(min_test_time, ds.start_test_time)
            except NotEnoughDataException as e:
                self.logger.warning(f'Not enough data for {pair=}. Skipping.')
                continue

            self.pairs.append(pair)
            self.pds[pair] = ds

        self.bs = None
        if residualize:
            self.bs = BetaStore(price_ts, min_test_time=min_test_time, hours_forward=1)

    def evaluate(self, forward_hour: int, opt: ModelOptions) -> pd.DataFrame:

        train_targets = {}
        test_targets = {}

        for pair, ds in self.pds.items():
            train_targets[pair], test_targets[pair] = ds.make_target(forward_hour=forward_hour)

        if self.bs:
            for pair in self.pairs:
                beta = self.bs.compute_beta(train_targets[pair])
                train_targets[pair] = self.bs.residualise(beta, train_targets[pair])
                test_targets[pair] = self.bs.residualise(beta, test_targets[pair])
                # print(f'{beta=}')

        if opt.demean:
            train_ret_df = pd.DataFrame(train_targets)
            test_ret_df = pd.DataFrame(test_targets)

            for pair in self.pairs:
                train_targets[pair] = train_targets[pair] - train_ret_df.mean(axis=1).loc[train_targets[pair].index]
                test_targets[pair] = test_targets[pair] - test_ret_df.mean(axis=1).loc[test_targets[pair].index]

        dfs = []

        for pair, ds in self.pds.items():
            res = fit_eval_model(ds.feature_df[ds.train_idx],
                                 ds.feature_df[ds.test_idx],
                                 train_targets[pair],
                                 test_targets[pair],
                                 opt)

            subdf_train = pd.concat([res.train.ypred, res.train.ytrue], axis=1)
            subdf_train['test'] = False
            subdf_test = pd.concat([res.test.ypred, res.test.ytrue], axis=1)
            subdf_test['test'] = True
            subdf = pd.concat([subdf_train, subdf_test], axis=0)
            subdf['pair'] = pair
            dfs.append(subdf)

        return pd.concat(dfs, axis=0)


class TestUniverseDataStore(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        coins = ['btc',
                 'ada',
                 'xrp',
                 'dot',
                 'doge',
                 'matic',
                 'algo',
                 'ltc',
                 'atom',
                 'link',
                 'near',
                 'bch',
                 'xlm',
                 'axs',
                 'vet',
                 'hbar',
                 'fil',
                 'egld',
                 'theta',
                 'icp',
                 'etc',
                 'xmr',
                 'xtz',
                 'aave',
                 'gala',
                 'grt',
                 'klay',
                 'cake',
                 'ar',
                 'eos',
                 'lrc',
                 'ksm',
                 'enj',
                 'qnt',
                 'amp',
                 'cvx',
                 'crv',
                 'mkr',
                 'xec',
                 'kda',
                 'tfuel',
                 'spell',
                 'sushi',
                 'bat',
                 'neo',
                 'celo',
                 'zec',
                 'osmo',
                 'chz',
                 'waves',
                 'dash',
                 'fxs',
                 'nexo',
                 'comp',
                 'mina',
                 'yfi',
                 'iotx',
                 'xem',
                 'snx',
                 'zil',
                 'rvn',
                 '1inch',
                 'gno',
                 'lpt',
                 'dcr',
                 'qtum',
                 'ens',
                 'icx',
                 'waxp',
                 'omg',
                 'ankr',
                 'scrt',
                 'sc',
                 'bnt',
                 'woo',
                 'zen',
                 'iost',
                 'btg',
                 'rndr',
                 'zrx',
                 'slp',
                 'anc',
                 'ckb',
                 'ilv',
                 'sys',
                 'uma',
                 'kava',
                 'ont',
                 'hive',
                 'perp',
                 'wrx',
                 'skl',
                 'flux',
                 'ren',
                 'mbox',
                 'ant',
                 'ray',
                 'dgb',
                 'movr',
                 'nu']

        coins = ['btc',
                 'ada',
                 'xrp',
                 'dot', ]
        universe = Universe(coins)

        start_date = datetime.datetime(year=2022, month=1, day=1)
        end_date = datetime.datetime(year=2023, month=1, day=1)

        time_col = 'Close time'

        df = load_universe_candles(universe, start_date, end_date, '5m')

        df.set_index(['pair', time_col], inplace=True)
        self.price_ts = ((df['Close'] + df['Open']) / 2.0).rename('price')

        super().__init__(*args, **kwargs)

    def test_b(self):
        bs = BetaStore(self.price_ts, 10000000000000000, 1)

    def test_a(self):
        decay_hours = [4, 12, 24, 48, 96]
        residualize = True

        uds = UniverseDataStore(self.price_ts, residualize, decay_hours)

        alpha = 1.0
        forward_hour = 24

        def get_lm():
            #     return Ridge(alpha=alpha)
            return Ridge(alpha=alpha)

        def transform_fit_target(y):
            return y

        def transform_model_after_fit(lm):
            return lm

        opt = ModelOptions(
            get_lm=get_lm,
            transform_fit_target=transform_fit_target,
            transform_model_after_fit=transform_model_after_fit,
            demean=True
        )
        rdf = uds.evaluate(forward_hour, opt)


def plot_eval(rdf, include_train: bool = False):
    f, axs = plt.subplots(1, 2, figsize=(20, 5));

    if include_train:
        cases = (True, False)
    else:
        cases = (True,)

    for test in cases:
        idx = rdf['test'] == test
        subdf = rdf.loc[idx]
        xx = subdf['ypred'] ** 2
        xy = subdf['ypred'] * subdf['ytrue']

        xxsum = xx.groupby(subdf.index).sum().cumsum()
        xysum = xy.groupby(subdf.index).sum().cumsum()
        t = pd.to_datetime(xxsum.index, unit='ms');

        axs[0].plot(t, xxsum, label='xxsum');
        axs[0].plot(t, xysum, label='xysum');

        totdf = subdf.groupby(subdf.index).sum(numeric_only=True)
        totdf['ypred'].plot(ax=axs[1]);
        (totdf['ytrue'] * totdf['ypred'].std() / totdf['ytrue'].std()).plot(ax=axs[1]);

    axs[0].legend();
    axs[0].grid();

    axs[1].legend();
    axs[1].grid();

    f.tight_layout();
