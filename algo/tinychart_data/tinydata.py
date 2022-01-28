import requests, pprint
import pandas as pd
import numpy as np


def all_assets():
    """Return list of all assets with their summary"""
    return requests.get(url='https://api.v3.tinychart.org/assets/').json()


def all_prices():
    """Return price and price one hour ago for all assets"""
    return requests.get(url='https://api.v3.tinychart.org/prices/').json()


def asset_summary(asset_id):
    """Return summary of asset_id"""
    return requests.get(url=f'https://api.v3.tinychart.org/asset/{asset_id}/').json()


def asset_current_price(asset_id):
    """Return current price and timestamp for asset_id"""
    return requests.get(url=f'https://api.v3.tinychart.org/asset/{asset_id}/price/')


def asset_historical_data(asset_id, asset_id2=0, start_timestamp=0):
    """Return historical price data for asset_id starting from start_timestamp"""
    return requests.get(url=f'https://api.v3.tinychart.org/asset/{asset_id}/{asset_id2}/prices/?start={start_timestamp}').json()


def processed_price_data(asset_id, start_timestamp=0):
    """Return processed data of historical prices"""
    raw_data = asset_historical_data(asset_id, start_timestamp=start_timestamp)
    candles = pd.DataFrame(raw_data['candles'])
    time_price = pd.DataFrame(raw_data['timestamps'])
    if not time_price.empty:
        price = np.append(candles['close'].values, time_price['price'].values)
        time = np.append(pd.to_datetime(candles['timestamp'], unit='s').values,
                         pd.to_datetime(time_price['timestamp'], unit='s').values)
    else:
        price = candles['close'].values
        time = pd.to_datetime(candles['timestamp'], unit='s').values
    result = pd.DataFrame({'datetime': time, 'price': price})
    return result.set_index('datetime')
