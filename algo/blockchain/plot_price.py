import matplotlib.pyplot as plt
import datetime
from tinyman.v1.client import TinymanMainnetClient


def plot_price(asset1_id: int, asset2_id: int, num_queries: int, timestamp: int = 0,
               inverse: bool = False, savefig: bool = False):
    client = TinymanMainnetClient()
    pool = client.fetch_pool(asset1_id, asset2_id)
    assert pool.exists

    data = list()
    for ps in query_pool_state_history(pool.address, num_queries, timestamp):
        data.append(ps)

    if not inverse:
        price = [p.asset2_reserves/p.asset1_reserves for p in data]
    else:
        price = [p.asset1_reserves/p.asset2_reserves for p in data]
    t = [datetime.datetime.fromtimestamp(p.time) for p in data]

    plt.xlabel('Time')
    if not inverse:
        plt.ylabel(f'{pool.asset2.unit_name} per {pool.asset1.unit_name}')
    else:
        plt.ylabel(f'{pool.asset1.unit_name} per {pool.asset2.unit_name}')
    plt.title(f'{pool.liquidity_asset.name}')
    plt.plot(t, price)
    if savefig:
        plt.savefig(f'{pool.asset1.unit_name}_{pool.asset2.unit_name}.png', bbox_inches="tight")
    else:
        plt.show()
