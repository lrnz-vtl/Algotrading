from matplotlib import pyplot as plt
from algo.universe.assets import get_asset_name, get_decimals
from algo.dataloading.caching import make_filter_from_universe, load_algo_pools
from algo.blockchain.wallets import Wallet
from algo.universe.universe import SimpleUniverse
import argparse, datetime
from algo.blockchain.mixedstream import TotalDataLoader
from definitions import ROOT_DIR
import os
from tinyman.v1.client import TinymanMainnetClient

REPORT_DIR = os.path.join(ROOT_DIR, 'reports')


def plot_value(address, df, filename):
    plt.figure(figsize=(12,5))
    plt.plot(df['time'].apply(datetime.datetime.fromtimestamp), df['value'] / 10**6)
    plt.xlabel('time')
    plt.ylabel('Total ALGO value')
    plt.title(address)
    plt.savefig(filename)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='price_cache_name', type=str, required=True)
    parser.add_argument('-a', dest='address', type=str, required=True)
    parser.add_argument('-o', dest='output', type=str, required=True)
    parser.add_argument('-u', dest='universe_cache_name', type=str, required=False)

    args = parser.parse_args()

    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = os.path.join(REPORT_DIR, f'{args.output}.pdf')

    price_cache = args.price_cache_name

    if args.universe_cache_name is not None:
        universe = SimpleUniverse.from_cache(args.universe_cache_name)
        ffilt = make_filter_from_universe(universe)
    else:
        ffilt = lambda x,y: x==0 or y==0

    dfp = TotalDataLoader(price_cache, TinymanMainnetClient(), ffilt).load()
    wallet = Wallet(args.address)

    value = wallet.portfolio_value(dfp)

    plot_value(args.address, value, fname)
