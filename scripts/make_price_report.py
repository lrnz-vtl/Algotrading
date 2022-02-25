from matplotlib import pyplot as plt
from algo.universe.assets import get_asset_name, get_decimals
from algo.blockchain.utils import load_algo_pools
from algo.dataloading.caching import make_filter_from_universe
from algo.strategy.analytics import process_market_df
from algo.universe.universe import SimpleUniverse
import argparse
from definitions import ROOT_DIR
import os

REPORT_DIR = os.path.join(ROOT_DIR, 'reports')


def price_report(df, filename):
    keys = ['time_5min', 'asset1', 'asset2']

    keyname = 'asset1'
    plot_keys = list(df[keyname].unique())

    def cumul_volume(subdf):
        subdf['algo_volume_cumul'] = subdf['algo_volume'].cumsum()
        return subdf

    df = df.groupby(['asset1', 'asset2']).apply(cumul_volume)

    cols = ('algo_price', 'algo_reserves', 'algo_volume_cumul')

    nrows = len(cols)
    ncols = len(plot_keys)
    f, axss = plt.subplots(nrows, ncols, figsize=(8 * ncols, 4 * nrows), sharex='col')

    for j, (asset_id, group) in enumerate(df.groupby([keyname])):
        asset_name = get_asset_name(asset_id)

        for i, col in enumerate(cols):
            ax = axss[i][j]
            ax.plot(group['time_5min'], group[col], label=col);
            if i == 0:
                ax.set_title(f'{asset_name}, {asset_id}');
            ax.legend();
            ax.grid();

            if i == len(cols) - 1:
                ax.tick_params(labelrotation=45, axis='x')

    f.tight_layout()
    plt.savefig(filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='price_cache_name', type=str, required=True)
    parser.add_argument('-v', dest='volume_cache_name', type=str, required=True)
    parser.add_argument('-n', dest='report_name', type=str, required=True)
    parser.add_argument('-u', dest='universe_cache_name', type=str, required=False)

    args = parser.parse_args()

    os.makedirs(REPORT_DIR, exist_ok=True)
    fname = os.path.join(REPORT_DIR, f'{args.report_name}.pdf')

    price_cache = args.price_cache_name
    volume_cache = args.volume_cache_name

    if args.universe_cache_name is not None:
        universe = SimpleUniverse.from_cache(args.universe_cache_name)
        ffilt = make_filter_from_universe(universe)
    else:
        ffilt = None

    dfp = load_algo_pools(price_cache, 'prices', ffilt)
    dfv = load_algo_pools(volume_cache, 'volumes', ffilt)
    df = process_market_df(dfp, dfv)

    price_report(df, fname)
