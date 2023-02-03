import logging
import os
import re
import time
import unittest
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import datetime
from joblib import Memory


cachedir = '/home/lorenzo/caches'
memory = Memory(cachedir, verbose=0)

basep = Path('/home/lorenzo/data/data.binance.vision')

RateLimitException = Exception

exclude_symbols = {'busd',
                   'dai',
                   'tusd',
                   'paxg'  # Gold
                   }

# Some ids correspond to multiple symbols
hardcoded_ids = {'gmx': 'gmx',
                 'bond': 'barnbridge',
                 'firo': 'zcoin',
                 'ont': 'ontology',
                 'sol': 'solana',
                 'dot': 'polkadot',
                 'lrc': 'loopring',
                 'shib': 'shiba-inu',
                 'xno': 'nano',
                 'ltc': 'litecoin',
                 'ftt': 'ftx-token',
                 'dydx': 'dydx',
                 'apt': 'aptos',
                 'eth': 'ethereum',
                 'bnb': 'binancecoin',
                 'ada': 'cardano',
                 'mana': 'decentraland',
                 'doge': 'dogecoin',
                 'xrp': 'ripple',
                 'uni': 'uniswap'
}


@memory.cache()
def symbol_to_ids() -> dict[str, list[str]]:
    url = 'https://api.coingecko.com/api/v3/coins/list'
    x = requests.get(url)
    coin_list = x.json()
    ret = {}
    for x in coin_list:
        symbol = x['symbol']
        if 'wormhole' in symbol:
            continue
        if 'binance-peg' in symbol:
            continue
        if symbol not in ret:
            ret[symbol] = [x['id']]
        else:
            ret[symbol].append(x['id'])
    return ret


def all_symbols():
    for y in (basep / '1d').glob('*USDT'):
        symbol = y.name[:-4].lower()
        if symbol.endswith('down') or symbol.endswith('up') or symbol.endswith('bear') or symbol.endswith('bull'):
            continue
        yield symbol


@memory.cache()
def get_mcap(coin_id: str, date: datetime.date) -> Optional[float]:
    logger = logging.getLogger(__name__)

    date_str = date.strftime("%d-%m-%Y")

    """ Memoize because of the rate limit """
    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/history?date={date_str}'
    oi_data = requests.get(url)
    oi_json = oi_data.json()
    if not oi_data.ok:
        if oi_json['status']['error_message'].startswith("You've exceeded the Rate Limit"):
            raise RateLimitException
        raise requests.RequestException(oi_json)
    if 'market_data' not in oi_json:
        logger.warning(f'market_data not in coin {coin_id}: {oi_json}')
        return None
    try:
        return oi_json['market_data']['market_cap']['usd']
    except KeyError as e:
        raise KeyError(oi_data) from e


class MultipleIdException(Exception):
    pass


def top_mcap(date: datetime.date, dry_run: bool = False) -> list[str]:
    logger = logging.getLogger(__name__)

    symbols_map = symbol_to_ids()
    ret = []

    for symbol in all_symbols():
        coin_ids = symbols_map.get(symbol, None)
        if coin_ids is None:
            logger.warning(f'{symbol} not in symbols_map')
            continue
        if symbol in exclude_symbols:
            continue

        if len(coin_ids) > 1:
            if symbol in hardcoded_ids:
                coin_id = hardcoded_ids[symbol]
                assert coin_id in coin_ids
            else:
                logger.error(f'{symbol=}, {coin_ids=} not hardcoded')
                continue
        else:
            coin_id = coin_ids[0]

        while True:
            try:
                if dry_run:
                    info = None
                else:
                    info = get_mcap(coin_id, date)
                if info is not None:
                    ret.append((symbol, info))
                break
            except RateLimitException as e:
                logger.info('Rate Limit Reached, sleeping for 5 seconds')
                time.sleep(5)

    return list(x[0] for x in sorted(ret, key=lambda x: x[1], reverse=True))


class TestSymbols(unittest.TestCase):
    def test_a(self):
        date = datetime.date(year=2022, month=12, day=1)
        top_mcap(date)


def load_candles(pair_name: str, freq: str):
    folder = basep / freq / pair_name / freq
    pattern = rf'{pair_name}-{freq}-(\d\d\d\d)-(\d\d).zip'
    p = re.compile(pattern)

    dfs = []

    columns = ['Open time',
               'Open',
               'High',
               'Low',
               'Close',
               'Volume',
               'Close time',
               'Quote asset volume',
               'Number of trades',
               'Taker buy base asset volume',
               'Taker buy quote asset volume',
               'Ignore'
               ]

    for filename in os.listdir(folder):
        filename = str(filename)

        if p.match(filename):
            csv_filename = filename.replace('.zip', '.csv')

            if not os.path.exists(folder / csv_filename):
                with zipfile.ZipFile(folder / str(filename), 'r') as zip_ref:
                    zip_ref.extractall(folder)

            subdf = pd.read_csv(folder / csv_filename, header=None)
            subdf.columns = columns
            dfs.append(subdf)

    df = pd.concat(dfs)
    df['pair'] = pair_name
    return df


class Universe:
    def __init__(self, coins):
        self.coins = coins

    @classmethod
    def make(cls, n_top_coins: int,
             mcap_date: datetime.date) -> 'Universe':
        return cls(top_mcap(mcap_date)[:n_top_coins])


def load_universe_candles(universe: Universe,
                          start_date: datetime.datetime,
                          end_date: datetime.datetime,
                          freq: str):
    logger = logging.getLogger(__name__)

    dfs = []

    for coin in universe.coins:
        pair_name = coin.upper() + 'USDT'

        subdf = load_candles(pair_name, freq)

        num_nans = subdf.isna().any(axis=1)
        if num_nans.sum() > 0:
            logger.warning(f"Dropping {num_nans.sum() / subdf.shape[0]} percentage of rows with nans for {pair_name}")
        subdf.dropna(inplace=True)

        subdf.sort_values(by='Open time', inplace=True)

        dfs.append(subdf)

    df = pd.concat(dfs)

    assert max(df['Close time'] - df['Close time'].astype(int)) == 0
    df['Close time'] = df['Close time'].astype(int)

    df['open_time'] = pd.to_datetime(df['Open time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['Close time'], unit='ms')

    idx = (df['close_time'] >= start_date) & (df['close_time'] <= end_date)
    return df[idx]
