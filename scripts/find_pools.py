from algo.universe.pools import QuickPoolInfoStore
from tinyman.v1.client import TinymanMainnetClient
import json
from pathlib import Path
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient

# get the 20 most liquid pools
qp = QuickPoolInfoStore(TinymanMainnetClient(), fromTinyman=True, max_query=2)
Path('caches/candidate_pools/pool_info').mkdir(parents=True, exist_ok=True)
with open('caches/candidate_pools/pool_info/top20_pools.json','w') as fp:
    json.dump(qp.asdicts(),fp, indent=2)

# get the 200 most liquid pools
qp = QuickPoolInfoStore(TinymanMainnetClient(), fromTinyman=True, max_query=20)
with open('caches/candidate_pools/pool_info/top200_pools.json','w') as fp:
    json.dump(qp.asdicts(),fp, indent=2)

# get all tinyman 1.1 pools
allpools11=QuickPoolInfoStore(TinymanMainnetClient())
with open('caches/candidate_pools/pool_info/all_pools.json','w') as fp:
    json.dump(allpools11.asdicts(),fp, indent=2)

# get all tinyman 1.0 pools
allpools10=QuickPoolInfoStore(TinymanOldnetClient(), old=True)
with open('caches/candidate_pools/pool_info/all_old_pools.json','w') as fp:
    json.dump(allpools10.asdicts(),fp, indent=2)
