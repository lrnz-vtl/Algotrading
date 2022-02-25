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


class FeatureSelect:

    def __init__(self, subcols):
        self.subcols = subcols

    def fit(self, X, y, **kwargs):
        return self

    def transform(self, X):
        return X[self.subcols]
