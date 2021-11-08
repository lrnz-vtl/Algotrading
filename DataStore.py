import datetime
import pandas as pd
import numpy as np
from TinyData import TinyData

class DataStore:
    """Up-to-date prices of all main coins and key metrics"""

    assets = [163650, 283820866, 31566704, 226701642, 320259224, 27165954, 312769,
              300208676, 384303832, 287867876, 230946361, 367058172, 137594422,
              310014962, 378382099, 137020565, 359383233, 297995609, 241759159]

    def __init__(self):
        """Set up the data store"""
        self.data = {}
        for asset_id in DataStore.assets:
            self.data[asset_id] = {}
        self.scraper = TinyData()
        self.fetch_data()

    def fetch_data(self):
        start=datetime.datetime.now().timestamp()
        for asset_id in DataStore.assets:
            n,_=self.get_name(asset_id)
            price = self.scraper.processed_price_data(asset_id)
            summary = self.scraper.asset_summary(asset_id)
            self.data[asset_id]['summary'] = {key: summary[key] for key in
                                               ['total_usd_reserves','supply',
                                                'circulating_supply','decimals',
                                                'volatility','change24h','transactions'] }
            self.data[asset_id]['price_history'] = price
            self.data[asset_id]['MA_2h'] = self.compute_moving_average(price, "2h")
            self.data[asset_id]['MA_15min'] = self.compute_moving_average(price, "15min")

        self.update_current_price()
        self.last_update_full = datetime.datetime.now().time()
        print(f'Updated price data in {datetime.datetime.now().timestamp()-start:.2f} seconds.')

    def update_current_price(self):
        instant_prices = self.scraper.all_prices()
        for asset_id in DataStore.assets:
            self.data[asset_id]['price_last'] = instant_prices['assets'][str(asset_id)]['price']
            self.data[asset_id]['price1h_last'] = instant_prices['assets'][str(asset_id)]['price1h']
        self.last_update_price = datetime.datetime.now().time()
        
    
    def get_name(self, asset_id):
        """Return the name and ticker for a given asset id"""
        return {
            0:         ('Algorand','ALGO'),
            163650:    ('Asia Reserve Currency Coin', 'ARCC'),
            283820866: ('Xfinite Entertainment Token', 'XET'),
            31566704:  ('USDC', 'USDC'),
            226701642: ('Yieldly', 'YLDY'),
            320259224: ('Wrapped Algo', 'wALGO'),
            27165954:  ('PLANET', 'Planets'),
            312769:    ('Tether USDt', 'USDt'),	
            300208676: ('Smile Coin', 'SMILE'),
            384303832: ('AKITA INU TOKEN', 'AKITA'),
            287867876: ('Opulous', 'OPUL'),
            230946361: ('AlgoGems', 'GEMS'),
            367058172: ('Realio Network LTD', 'RST'),
            137594422: ('HEADLINE', 'HDL'),
            310014962: ('AlcheCoin', 'ALCH'),
            378382099: ('Tinychart Token', 'TINY'),
            137020565: ('Buy Token', 'BUY'),
            359383233: ('Cogmento', 'COGS'),
            297995609: ('Choice Coin', 'Choice'),
            241759159: ('Freckle', 'FRKL'),
        }.get(asset_id, (None,None))
    
    def compute_moving_average(self, df, interval="3h"):
        """Compute the moving average from processed_price_data"""
        return df.resample(interval).mean().fillna(0).rolling(window=3, min_periods=1).mean()
