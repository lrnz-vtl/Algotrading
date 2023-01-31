import unittest

import numpy as np
import sklearn
from sklearn import decomposition
import pandas as pd
from matplotlib import pyplot as plt
import datetime


class BetaStore:
    def __init__(self, ret_df: pd.DataFrame):
        """ Index is (pair,time) """

        ret_df = ret_df.loc[(ret_df.isna().sum(axis=1) <= (ret_df.shape[0] // 2))].dropna(axis=1)

        pca = sklearn.decomposition.PCA(n_components=1)
        mkt_returns = pca.fit_transform(ret_df)[:, 0]

        self.mkt_returns = pd.Series(mkt_returns, index=ret_df.index).rename('mkt_return')

    def compute_beta(self, product_ret_ts: pd.Series) -> float:
        idx = self.mkt_returns.index.intersection(product_ret_ts.index)
        assert len(idx) > 0

        x = self.mkt_returns[idx]
        y = product_ret_ts[idx]

        return (x * y).sum() / (x * x).sum()

    def residualise(self, beta: float, product_ret_ts: pd.Series) -> pd.Series:
        mkt_component = beta * self.mkt_returns[product_ret_ts.index].fillna(0)
        return product_ret_ts - mkt_component


class TestBetas(unittest.TestCase):

    def test_a(self):
        start_date = datetime.datetime(year=2022, month=7, day=1)
        end_date = datetime.datetime(year=2023, month=1, day=1)
        mcap_date = datetime.date(year=2022, month=12, day=1)

        bc = BetaComputer(start_date, end_date, mcap_date)
        x = bc.pca.explained_variance_ratio_
        plt.plot(x, np.zeros(x.shape), 'b+', ms=20)
        plt.show()

        plt.figure(figsize=(12, 7))

        bc.ret0.plot(label='comp0')
        bc.df['BTCUSDT'].plot(label='btc')
        plt.legend()
        plt.grid()
        print(np.corrcoef(bc.ret0, bc.df['BTCUSDT'])[0, 1])
        plt.show()

        print(bc.betas.sort_values())
