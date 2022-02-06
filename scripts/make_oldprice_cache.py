import datetime
from algo.blockchain.process_prices import PriceCacher
from tinyman_old.v1.client import TinymanMainnetClient as TinymanOldnetClient

universe_cache_name = '20220202-110531'

cache_name = '20220205_prehack'

date_min = datetime.datetime(year=2021, month=10, day=8)
date_max = datetime.datetime(year=2021, month=12, day=31)

print(date_max.isoformat())

pc = PriceCacher(client=TinymanOldnetClient(),
                 universe_cache_name=universe_cache_name,
                 date_min=date_min,
                 date_max=date_max
                 )
pc.cache(cache_name)

