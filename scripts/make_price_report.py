from matplotlib import pyplot as plt
from algo.universe.assets import get_asset_name, get_decimals
from algo.blockchain.utils import load_algo_pools
from algo.strategy.analytics import process_market_df
import argparse


def process(subdf, asset_id):
    decimals = get_decimals(asset_id)
    subdf['price_norm'] = subdf['price'] * 10 ** (decimals - 6)
    subdf['algo_volume_cumul'] = subdf['algo_volume'].cumsum()
    return subdf


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='price_cache_name', type=str, required=True)
    parser.add_argument('-v', dest='volume_cache_name', type=str, required=True)

    args = parser.parse_args()

    price_cache = args.price_cache_name
    volume_cache = args.volume_cache_name

    dfp = load_algo_pools(price_cache, 'prices')
    dfv = load_algo_pools(volume_cache, 'volumes')
    df = process_market_df(dfp, dfv)

    keys = ['time_5min', 'asset1', 'asset2']

    df['price'] = df['asset2_reserves'] / df['asset1_reserves']
    df['algo_volume'] = df['asset2_amount'] / (10 ** 6)
    df['algo_reserves'] = df['asset2_reserves'] / (10 ** 6)
    df = df.groupby('asset1').apply(lambda x: process(x, x.name))

    keyname = 'asset1'
    plot_keys = list(df[keyname].unique())

    cols = ('price_norm', 'algo_reserves', 'algo_volume_cumul')

    nrows = len(cols)
    ncols = len(plot_keys)
    f, axss = plt.subplots(nrows, ncols, figsize=(8 * ncols, 4 * nrows), sharex='col')

    for j, (asset_id, group) in enumerate(df.groupby([keyname])):
        asset_name = get_asset_name(asset_id)

        for i, col in enumerate(cols):
            ax = axss[i][j]
            ax.plot(group['time_5min'], group[col], label=col);
            ax.set_title(f'{asset_name}, {asset_id}');
            ax.legend();
            ax.grid();

    plt.xticks(rotation=35);
    plt.savefig('price_report.pdf')
