import datetime
from algo.blockchain.process_prices import PriceCacher
from tinyman.v1.client import TinymanMainnetClient
import asyncio

universe_cache_name = '20220202-110531'

cache_name = '20220204_async'

date_min = datetime.datetime(year=2022, month=1, day=20)

pc = PriceCacher(client=TinymanMainnetClient(),
                 universe_cache_name=universe_cache_name,
                 date_min=date_min,
                 date_max=None
                 )
pc.cache(cache_name)

