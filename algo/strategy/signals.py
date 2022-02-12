from matplotlib import pyplot as plt
from algo.blockchain.utils import load_algo_pools
from algo.strategy.analytics import *
from ts_tools_algo.features import exp_average
from typing import Optional
from typing import Callable
import unittest
from scipy.stats.mstats import winsorize
from sklearn.linear_model import LinearRegression


def make_ma_feature(ts: pd.Series, minutes: int):
    col_name = f'ma_{minutes}'
    ma = pd.Series(exp_average(ts, minutes * 60), index=ts.index, name=col_name)
    return (ts - ma) / ts


def mask_nans(*vecs):
    mask = ~np.any([np.isnan(x) for x in vecs], axis=0)
    return tuple((x[mask] for x in vecs))


class AnalysisDataStore:

    def __init__(self, price_cache: str, volume_cache: str,
                 filter_liq: Optional[float],
                 filter_asset: set[int]
                 ):

        dfp = load_algo_pools(price_cache, 'prices')
        dfv = load_algo_pools(volume_cache, 'volumes')

        df = process_market_df(dfp, dfv)
        df = df.set_index(['asset1', 'time_5min']).sort_index()
        assert np.all(df['asset2'] == 0)
        df = df.drop(columns=['asset2'])

        # Filter on liquidity
        if filter_liq is not None:
            df = df[df['algo_reserves'] > filter_liq]

        df = df[~df.index.get_level_values('asset1').isin(filter_asset)]

        ws = make_weights(df)
        weights = df[['date']].merge(ws, on=['asset1', 'date'], how='left')
        weights.index = df.index
        weights = weights['weight']

        self.df = df
        self.weights = weights

    def bootstrap_arrays(self, N, *vecs):
        assets = self.df.index.get_level_values(0).unique()
        for i in range(N):
            boot_assets = np.random.choice(assets, len(assets))
            yield tuple(
                np.concatenate([vec[self.df.index.get_loc(boot_asset)] for boot_asset in boot_assets]) for vec in vecs)

    def eval_feature(self,
                     featurizer: Callable[[pd.DataFrame], pd.Series],
                     response_maker: Callable[[pd.DataFrame], pd.Series]):

        feature = self.df.groupby(['asset1']).apply(featurizer)
        response = response_maker(self.df)

        N = 100
        betas = np.zeros(N)

        for i, (x, y, w) in enumerate(self.bootstrap_arrays(N, feature, response, self.weights)):
            x, y, w = mask_nans(x, y, w)
            betas[i] = LinearRegression().fit(x[:, None], y, w).coef_

        plt.grid();
        plt.hist(betas);
        plt.show();

        x, y, w = mask_nans(feature, response, self.weights)
        beta = LinearRegression().fit(x.values[:, None], y, w).coef_
        sig = x * beta

        xxw = w * sig ** 2
        xyw = w * sig * y

        ids = self.df.index.get_level_values(0).unique()
        cols = len(ids)
        f, axs = plt.subplots(1, cols, figsize=(10 * cols, 4))

        for asset, ax in zip(ids, axs):
            xxw.loc[asset].groupby('time_5min').sum().cumsum().plot(label='signal', ax=ax)
            xyw.loc[asset].groupby('time_5min').sum().cumsum().plot(label='response', ax=ax)
            ax.set_title(asset)
            ax.legend()
            ax.grid()

        plt.show()


class TestAnalysisDs(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        price_cache = '20220209_prehack'
        volume_cache = '20220209_prehack'
        filter_liq = 10000
        # remove Birdbot
        filter_asset = {478549868, }

        self.ds = AnalysisDataStore(price_cache, volume_cache, filter_liq, filter_asset)
        super().__init__(*args, **kwargs)

    def test_feature(self):
        def featurizer(df):
            return make_ma_feature(df.droplevel(0)['algo_price'], 30)

        def response_maker(df):
            resp = make_response(df, 30)
            resp_winsor = resp.copy()
            mask = ~np.isnan(resp_winsor)
            resp_winsor[mask] = winsorize(resp_winsor[mask], limits=(0.05, 0.05))
            return resp_winsor

        self.ds.eval_feature(featurizer, response_maker)
