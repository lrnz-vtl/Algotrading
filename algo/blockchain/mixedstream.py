from __future__ import annotations
from algo.blockchain.stream import DataStream, PriceVolumeStream, only_price
from algo.blockchain.process_prices import PriceScraper
from algo.blockchain.stream import PriceUpdate
import aiohttp
from tinyman.v1.client import TinymanClient
import asyncio
import logging
from algo.blockchain.process_prices import PoolState
from algo.blockchain.algo_requests import QueryParams
from algo.universe.universe import SimpleUniverse
import datetime
import uvloop


class PriceStreamer:
    def __init__(self,
                 universe: SimpleUniverse,
                 client: TinymanClient,
                 date_min: datetime.datetime,
                 filter_tx_type: bool = True):
        self.client = client
        self.pools = [(x.asset1_id, x.asset2_id) for x in universe.pools]
        self.date_min = date_min

        self.filter_tx_type = filter_tx_type

        self.data: list[PriceUpdate] = []

        self.logger = logging.getLogger(__name__)

    def load(self) -> list[PriceUpdate]:
        async def main():
            async with aiohttp.ClientSession() as session:
                await asyncio.gather(*[self._load_pool(session, assets) for assets in self.pools])

        uvloop.install()
        asyncio.run(main())

        self.data.sort(key=lambda x: x.price_update.time)
        return self.data

    async def _load_pool(self, session, assets) -> None:
        assets = list(sorted(assets, reverse=True))

        scraper = PriceScraper(self.client, assets[0], assets[1], skip_same_time=False)

        if scraper is None:
            self.logger.error(f'Pool for assets {assets[0], assets[1]} does not exist')
            return

        async for pool_state in scraper.scrape(session=session,
                                               num_queries=None,
                                               timestamp_min=None,
                                               query_params=QueryParams(after_time=self.date_min),
                                               filter_tx_type=self.filter_tx_type):
            self.data.append(PriceUpdate(asset_ids=(max(assets), min(assets)), price_update=pool_state))


class MixedPriceStreamer:
    def __init__(self, universe: SimpleUniverse, date_min: datetime.datetime, client: TinymanClient,
                 filter_tx_type: bool = True):

        self.universe = universe
        self.pvs = None
        self.date_min = date_min
        self.client = client
        self.filter_tx_type = filter_tx_type

    def scrape(self):
        if not self.pvs:
            max_block = -1
            ps = PriceStreamer(self.universe, self.client, date_min=self.date_min, filter_tx_type=self.filter_tx_type)
            for x in ps.load():
                yield x
                assert x.price_update.block >= max_block
                max_block = x.price_update.block
            query_params = QueryParams(min_block=max_block + 1)
            ds = DataStream(self.universe, query_params)
            self.pvs = PriceVolumeStream(ds)
        else:
            yield from only_price(self.pvs.scrape())
