from typing import Optional
import datetime
import aiohttp


async def query_transactions(session:aiohttp.ClientSession, params: dict, num_queries: Optional[int], before_time: Optional[datetime.datetime]):

    query = f'https://algoindexer.algoexplorerapi.io/v2/transactions'

    if before_time is not None:
        params = {**params, **{'before-time': before_time.strftime('%Y-%m-%d')}}

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
            # resp = requests.get(query, params={**params, **{'next': resp['next-token']}}).json()
        else:
            resp = None
        i += 1