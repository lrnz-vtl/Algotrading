import logging
from matplotlib import pyplot as plt
import unittest
from algo.signals.evaluation import AnalysisDataStore
from algo.signals.featurizers import MAPriceFeaturizer, concat_featurizers
from algo.signals.responses import SimpleResponse
from sklearn.linear_model import LinearRegression


class TestAnalysisDs(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        price_cache = '20220209_prehack'
        volume_cache = '20220209_prehack'
        filter_liq = 10000
        # remove Birdbot
        filter_asset = {478549868, }

        logging.basicConfig()

        self.ds = AnalysisDataStore(price_cache, volume_cache, filter_liq, filter_asset)
        respMaker = SimpleResponse(30)
        self.response = self.ds.make_response(respMaker)

        minutes = (30, 60, 120)
        self.featurizers = [MAPriceFeaturizer(m) for m in minutes]

        super().__init__(*args, **kwargs)

    def test_features(self):

        f,axs = plt.subplots(1,3, figsize=(10, 5))

        for featurizer,ax in zip(self.featurizers,axs):
            betas = self.ds.eval_feature(self.ds.make_asset_features(featurizer), self.response)

            ax.hist(betas)
            ax.set_title(f'minutes = {featurizer.minutes}')
            ax.grid()
        plt.show();

    def test_model(self):
        features = self.ds.make_asset_features(concat_featurizers(self.featurizers))
        model = LinearRegression()
        self.ds.eval_model(model, features, self.response, filter_nans=True)
