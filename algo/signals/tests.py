import logging
from matplotlib import pyplot as plt
import unittest
from algo.signals.datastore import AnalysisDataStore, RollingLiquidityFilter
from algo.signals.evaluation import *
from algo.signals.featurizers import MAPriceFeaturizer, concat_featurizers
from algo.signals.responses import SimpleResponse, WinsorizeResponse
from algo.signals.weights import SimpleWeightMaker
from sklearn.linear_model import LinearRegression
from algo.universe.universe import SimpleUniverse


class TestAnalysisDs(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        price_cache = '20220209_prehack'
        # volume_cache = '20220209_prehack'
        smalluniverse_cache_name = 'liquid_algo_pools_nousd_prehack'

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        ffill_price_minutes = 10

        universe = SimpleUniverse.from_cache(smalluniverse_cache_name)

        self.ds = AnalysisDataStore(price_cache, None, universe, SimpleWeightMaker(),
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
