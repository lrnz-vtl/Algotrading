import datetime
import logging
import unittest
from sklearn.linear_model import Ridge
from sklearn.preprocessing import RobustScaler

from algo.binance.coins import Universe, load_universe_candles, basep, all_symbols, top_mcap, symbol_to_ids
from algo.binance.fit import UniverseDataStore, ModelOptions, ResidOptions, UniverseFitOptions


class TestUniverseDataStore(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        coins = ['btc',
                 'ada',
                 'xrp',
                 'dot',
                 'doge',
                 'matic',
                 'algo',
                 'ltc',
                 'atom',
                 'link',
                 'near',
                 'bch',
                 'xlm',
                 'axs',
                 'vet',
                 'hbar',
                 'fil',
                 'egld',
                 'theta',
                 'icp',
                 'etc',
                 'xmr',
                 'xtz',
                 'aave',
                 'gala',
                 'grt',
                 'klay',
                 'cake',
                 'ar',
                 'eos',
                 'lrc',
                 'ksm',
                 'enj',
                 'qnt',
                 'amp',
                 'cvx',
                 'crv',
                 'mkr',
                 'xec',
                 'kda',
                 'tfuel',
                 'spell',
                 'sushi',
                 'bat',
                 'neo',
                 'celo',
                 'zec',
                 'osmo',
                 'chz',
                 'waves',
                 'dash',
                 'fxs',
                 'nexo',
                 'comp',
                 'mina',
                 'yfi',
                 'iotx',
                 'xem',
                 'snx',
                 'zil',
                 'rvn',
                 '1inch',
                 'gno',
                 'lpt',
                 'dcr',
                 'qtum',
                 'ens',
                 'icx',
                 'waxp',
                 'omg',
                 'ankr',
                 'scrt',
                 'sc',
                 'bnt',
                 'woo',
                 'zen',
                 'iost',
                 'btg',
                 'rndr',
                 'zrx',
                 'slp',
                 'anc',
                 'ckb',
                 'ilv',
                 'sys',
                 'uma',
                 'kava',
                 'ont',
                 'hive',
                 'perp',
                 'wrx',
                 'skl',
                 'flux',
                 'ren',
                 'mbox',
                 'ant',
                 'ray',
                 'dgb',
                 'movr',
                 'nu']

        coins = coins[:20]
        universe = Universe(coins)

        start_date = datetime.datetime(year=2022, month=1, day=1)
        end_date = datetime.datetime(year=2023, month=1, day=1)

        time_col = 'Close time'

        df = load_universe_candles(universe, start_date, end_date, '5m')

        df.set_index(['pair', time_col], inplace=True)
        self.price_ts = ((df['Close'] + df['Open']) / 2.0).rename('price')

        super().__init__(*args, **kwargs)

    def test_a(self):
        decay_hours = [1/12, 4, 12, 24, 48, 96]
        ro = ResidOptions(market_pairs={'BTCUSDT'})

        uds = UniverseDataStore(self.price_ts, decay_hours, ro)

        alpha = 1.0

        def get_lm():
            return Ridge(alpha=alpha)

        def transform_fit_target(y):
            return y

        def transform_model_after_fit(lm):
            return lm

        global_opt = ModelOptions(
            get_lm=lambda: Ridge(alpha=0),
            transform_fit_target=transform_fit_target,
            transform_model_after_fit=transform_model_after_fit,
        )
        # global_opt = None

        opt = ModelOptions(
            get_lm=get_lm,
            transform_fit_target=transform_fit_target,
            transform_model_after_fit=transform_model_after_fit,
        )
        fit_options = UniverseFitOptions(demean=True,
                                         global_model_options=global_opt,
                                         forward_hour=24,
                                         target_scaler=lambda: RobustScaler()
                                         )

        global_res = uds.fit_global(fit_options)
        uds.fit_products(global_res, opt)


class TestSymbols(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
        super().__init__(*args, **kwargs)

    def test_a(self):
        symbols_map = symbol_to_ids()

        for symbol in all_symbols():
            if symbol == 'eth':
                coin_id = symbols_map.get(symbol, None)
                print(f'{symbol=}, {coin_id=}')

    def test_b(self):
        top_mcap(datetime.date(year=2022, month=1, day=1), dry_run=True)


if __name__ == '__main__':
    unittest.main()
