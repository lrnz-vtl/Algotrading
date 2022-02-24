import unittest
from algo.signals.datastore import AnalysisDataStore, RollingLiquidityFilter
from algo.signals.evaluation import *
from algo.signals.featurizers import MAPriceFeaturizer, concat_featurizers
from algo.signals.responses import SimpleResponse, WinsorizeResponse
from algo.signals.weights import SimpleWeightMaker
from sklearn.linear_model import LinearRegression
from algo.universe.universe import SimpleUniverse
from pydantic import BaseModel

smalluniverse_cache_name = 'liquid_algo_pools_nousd_prehack_noeth'


class EMALinearStrategy(BaseModel):
    betas: dict[int, float]


class TestAnalysisDs(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        price_cache = '20220209_prehack'
        # volume_cache = '20220209_prehack'

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        ffill_price_minutes = 10

        universe = SimpleUniverse.from_cache(smalluniverse_cache_name)

        self.ds = AnalysisDataStore([price_cache], [], universe, SimpleWeightMaker(),
                                    ffill_price_minutes=ffill_price_minutes)

        self.minutes = (30, 60, 120)
        self.featurizers = [MAPriceFeaturizer(m) for m in self.minutes]

        respMaker = SimpleResponse(120, 5)
        features = self.ds.make_asset_features(concat_featurizers(self.featurizers))
        response = self.ds.make_response(respMaker)

        self.fitds = self.ds.make_fittable_data(features, response, [RollingLiquidityFilter()], [], True)

        super().__init__(*args, **kwargs)

    def test_features(self):
        cols = len(self.featurizers)
        f, axs = plt.subplots(1, cols, figsize=(4 * cols, 5))

        for betas, ax, minutes in zip(self.fitds.bootstrap_betas(), axs, self.minutes):
            ax.hist(betas)
            ax.set_title(f'minutes = {minutes}')
            ax.grid()
        plt.show();

    def test_fit_and_validate_model(self):
        model = WinsorizeResponse(LinearRegression())

        train_idx, test_idx = self.fitds.make_train_val_splits()
        self.fitds.fit_and_eval_model(model, train_idx, test_idx)

        self.logger.info(f'betas = {model.coef_}')


class TestOOS(unittest.TestCase):

    def _make_fitds(self, price_cache: str):
        ds = AnalysisDataStore([price_cache], [], self.universe, SimpleWeightMaker(),
                               ffill_price_minutes=self.ffill_price_minutes)
        features = ds.make_asset_features(concat_featurizers(self.featurizers))
        response = ds.make_response(self.respMaker)
        return ds.make_fittable_data(features, response, [RollingLiquidityFilter()], [], True)

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.ffill_price_minutes = 10

        self.universe = SimpleUniverse.from_cache(smalluniverse_cache_name)

        self.minutes = (30, 60, 120)
        self.featurizers = [MAPriceFeaturizer(m) for m in self.minutes]
        self.respMaker = SimpleResponse(120, 5)

        self.model = WinsorizeResponse(LinearRegression())

        super().__init__(*args, **kwargs)

    def test_oos(self):
        price_cache = '20220209_prehack'
        ds = self._make_fitds(price_cache)
        self.model = ds.fit_model(self.model, ds.full_idx)
        self.logger.info(f'betas = {self.model.coef_}')

        price_cache_oos = '20220209'
        ds_oos = self._make_fitds(price_cache_oos)
        ds_oos.test_model(self.model, ds_oos.full_idx)


class TestFitTrading(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.cache_names = ['20220209_prehack', '20220209']

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.ffill_price_minutes = 10

        self.universe = SimpleUniverse.from_cache(smalluniverse_cache_name)

        self.minutes = (30, 60, 120)
        self.featurizers = [MAPriceFeaturizer(m) for m in self.minutes]
        self.respMaker = SimpleResponse(120, 5)

        self.model = WinsorizeResponse(LinearRegression())

        super().__init__(*args, **kwargs)

    def test_fit_all(self):
        ds = AnalysisDataStore(self.cache_names, [], self.universe, SimpleWeightMaker(),
                               ffill_price_minutes=self.ffill_price_minutes)
        features = ds.make_asset_features(concat_featurizers(self.featurizers))
        response = ds.make_response(self.respMaker)
        fitds = ds.make_fittable_data(features, response, [RollingLiquidityFilter()], [], True)

        model = WinsorizeResponse(LinearRegression())

        cols = len(self.featurizers)
        f, axs = plt.subplots(1, cols, figsize=(4 * cols, 5))

        for betas, ax, minutes in zip(fitds.bootstrap_betas(), axs, self.minutes):
            ax.hist(betas)
            ax.set_title(f'minutes = {minutes}')
            ax.grid()
        plt.show()

        model = fitds.fit_model(model, fitds.full_idx)
        self.logger.info(model.coef_)

        fitds.test_model(model, fitds.full_idx)
