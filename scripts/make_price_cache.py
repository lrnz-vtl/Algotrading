import datetime
from algo.blockchain.cache import PriceDataCacher

universe_cache_name = '20220202-110531'

cache_name = '20220204'

date_min = datetime.datetime(year=2022, month=1, day=20)

pc = PriceDataCacher(universe_cache_name=universe_cache_name, date_min=date_min)
pc.cache(cache_name)

