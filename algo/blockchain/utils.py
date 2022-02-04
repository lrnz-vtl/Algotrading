import pandas as pd
import requests
from typing import Optional
import datetime


def query_transactions(params: dict, num_queries: Optional[int]):
    query = f'https://algoindexer.algoexplorerapi.io/v2/transactions'
    resp = requests.get(query, params=params).json()

    i = 0
    while resp and (num_queries is None or i < num_queries):
        for tx in resp['transactions']:
            yield tx
        resp = requests.get(query, params={**params, **{'next': resp['next-token']}}).json()
        i += 1


def datetime_to_int(t: datetime.datetime):
    return int(t.timestamp())


def generator_to_df(gen, time_columns=('time',)):
    df = pd.DataFrame(gen)
    for col in time_columns:
        df[col] = pd.to_datetime(df[col], unit='s', utc=True)
    return df
