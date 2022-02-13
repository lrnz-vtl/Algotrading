from algo.blockchain.stream import PriceVolumeStream
import time

blockid=19226712

pvs=PriceVolumeStream(blockid, '../caches/candidate_pools/pool_info/top20_pools.json')
ti = time.time()
pvs.scrape()
print(f'Scraped data since block {blockid} in {tiime.time()-ti} seconds.')
for i in range(10):
    time.sleep(10)
    ti = time.time()
    pvs.scrape()
    print(f'Scraped 10 seconds of data in {time.time()-ti} seconds.')

print(pvs.prices)
