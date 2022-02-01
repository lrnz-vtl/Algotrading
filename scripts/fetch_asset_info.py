import logging
from algo.universe.assets import AssetMarketDataStore
from tinyman.v1.client import TinymanMainnetClient
from algo.universe.hardcoded import verified_assets

logging.basicConfig(level=logging.INFO)


client = TinymanMainnetClient()
ds = AssetMarketDataStore(client, verified_assets)

for x in ds.data:
    print(x)
