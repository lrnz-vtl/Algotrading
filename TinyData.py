import requests, pprint
import pandas as pd
import numpy as np

class TinyData:
    """Import Tinyman data from TinyChart. Prices expressed in Algorand"""

    def all_assets(self):
        """Return list of all assets with their summary"""
        return requests.get(url='https://api.tinychart.org/assets/').json()

    def all_prices(self):
        """Return price and price one hour ago for all assets"""
        return requests.get(url='https://api.tinychart.org/prices/').json()

    def asset_summary(self, asset_id):
        """Return summary of asset_id"""
        return requests.get(url=f'https://api.tinychart.org/asset/{asset_id}/').json()

    def asset_current_price(self, asset_id):
        """Return current price and timestamp for asset_id"""
        return requests.get(url=f'https://api.tinychart.org/asset/{asset_id}/price/')

    def asset_historical_data(self, asset_id, start_timestamp=0):
        """Return historical price data for asset_id starting from start_timestamp"""
        return requests.get(url=f'https://api.tinychart.org/asset/{asset_id}/prices/?start={start_timestamp}').json()

    def processed_price_data(self, asset_id, start_timestamp=0):
        """Return processed data of historical prices"""
        raw_data = self.asset_historical_data(asset_id, start_timestamp)
        candles = pd.DataFrame(raw_data['candles'])
        time_price = pd.DataFrame(raw_data['timestamps'])
        price=np.append(candles['close'].values, time_price['price'].values)
        time=np.append(pd.to_datetime(candles['timestamp'],unit='s').values,
                       pd.to_datetime(time_price['timestamp'],unit='s').values)
        result=pd.DataFrame({'datetime':time,'price':price})
        return result.set_index('datetime')
