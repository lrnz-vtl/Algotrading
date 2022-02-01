from tinyman.v1.client import TinymanMainnetClient

asset1 = 0
asset2 = 470842789

client = TinymanMainnetClient()
pool = client.fetch_pool(asset1, asset2)
print(pool.asset1_reserves, pool.asset2_reserves)