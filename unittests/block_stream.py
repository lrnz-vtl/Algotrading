from algo.blockchain.stream import PriceVolumeStream
from algo.strategy.analytics import process_market_df
import time


blockid=19241100

pvs=PriceVolumeStream(blockid, '../caches/candidate_pools/pool_info/top20_pools.json')
ti = time.time()
pvs.scrape()
print(f'Scraped data since block {blockid} in {time.time()-ti} seconds.')
for i in range(10):
    time.sleep(10)
    ti = time.time()
    pvs.scrape()
    print(f'Scraped 10 seconds of data in {time.time()-ti} seconds.')

prices=pvs.prices()
volumes=pvs.volumes()

# remove pools without algo
prices = prices[prices['asset2']==0]
volumes = volumes[volumes['asset2']==0]

market_data = process_market_df(prices, volumes)
