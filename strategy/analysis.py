from analysis.timeseries import ExpAverages

class AnalyticProvider:
    def __init__(self, datastore, time_scale_long=10000, time_scale_short=1000):
        """Take a datastore and compute two EMA to be used in strategy"""
        self.datastore = datastore
        self.time_scale_long = time_scale_long
        self.time_scale_short = time_scale_short
        self.expavg_long = {}
        self.expavg_short = {}
        self.update()

    def update(self):
        """Using current datastore state, compute relevant metrics"""
        for assetid in self.datastore:
            price=self.datastore[assetit].slow.price_history['price']
            #time=self.datastore[assetid].slow.price_history.index.view(np.int64).astype(float)/10**9
            self.expavg_long[assetid]=ExpAverages(price,self.time_scale_long)
            self.expavg_short[assetid]=ExpAverages(price,self.time_scale_short)
