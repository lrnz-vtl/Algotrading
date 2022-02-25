import pandas as pd


class RemoveIntercept:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def fit(self, X, y, **kwargs):
        self.pipeline.fit(X, y, **kwargs)
        return self

    def predict(self, X):
        return self.pipeline.predict(X) - self.pipeline.intercept_

    def __getattr__(self, attr):
        if attr == 'fit':
            return self.fit
        elif attr == 'predict':
            return self.predict
        elif attr == 'fit_predict':
            raise NotImplementedError
        else:
            return getattr(self.pipeline, attr)


class Demean:
    def __init__(self, pipeline, weights):
        self.pipeline = pipeline
        self.weights = weights
        self.wsums = weights.groupby('time_5min').sum()

    def predict(self, X: pd.DataFrame):
        y = pd.Series(self.pipeline.predict(X), index=X.index)
        ywsums = (y * self.weights).groupby('time_5min').sum()
        return y - ywsums/self.wsums

    def __getattr__(self, attr):
        if attr == 'predict':
            return self.predict
        elif attr == 'fit_predict':
            raise NotImplementedError
        else:
            return getattr(self.pipeline, attr)


class FeatureSelect:

    def __init__(self, subcols):
        self.subcols = subcols

    def fit(self, X, y, **kwargs):
        return self

    def transform(self, X):
        return X[self.subcols]
