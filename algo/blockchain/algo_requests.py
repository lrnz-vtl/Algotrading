from typing import Optional
import datetime
import aiohttp
from dataclasses import dataclass
import requests


@dataclass
class QueryParams:
    after_time: Optional[datetime.date] = None
    before_time: Optional[datetime.date] = None
    min_block: Optional[int] = None

    def make_params(self):
        params = {}
        if self.before_time is not None:
            params['before-time'] = self.before_time.strftime('%Y-%m-%d')
        if self.after_time is not None:
            params['after-time'] = self.after_time.strftime('%Y-%m-%d')
        if self.min_block is not None:
            params['min-round'] = self.min_block
        return params


def get_current_round():
    url = f'https://algoindexer.algoexplorerapi.io/v2/transactions'
    req = requests.get(url=url).json()
    return int(req['current-round'])


async def query_transactions(session: aiohttp.ClientSession,
                             params: dict,
                             num_queries: Optional[int],
                             query_params: QueryParams):

    query = f'https://algoindexer.algoexplorerapi.io/v2/transactions'

    params = {**params, **query_params.make_params()}

    async with session.get(query, params=params) as resp:
        resp = await resp.json()

    i = 0
    while resp and (num_queries is None or i < num_queries):
        if 'transactions' not in resp:
            print(f"'transactions' key not in resp:{resp}")
        else:
            for tx in resp['transactions']:
                yield tx

        if 'next-token' in resp:
            async with session.get(query, params={**params, **{'next': resp['next-token']}}) as resp:
                resp = await resp.json()
        else:
            resp = None
        i += 1