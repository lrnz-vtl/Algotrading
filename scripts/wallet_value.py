from matplotlib import pyplot as plt
from algo.universe.assets import get_asset_name, get_decimals
from algo.blockchain.utils import load_algo_pools, make_filter_from_universe
from algo.blockchain.wallets import Wallet
from algo.universe.universe import SimpleUniverse
import argparse, datetime
from definitions import ROOT_DIR
import os

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
        ffilt = None

    dfp = load_algo_pools(price_cache, 'prices', ffilt)
    wallet = Wallet(args.address)

    value = wallet.portfolio_value(dfp)

    plot_value(args.address, value,fname)
