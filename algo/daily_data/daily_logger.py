import requests
from typing import Optional, Mapping
from dataclasses import dataclass
from algo.tools.timestamp import Timestamp
from algo.sqlite.base import BaseSqliteLogger


@dataclass
class DailyPoolData:
    asset1: int
    asset2: int
    timestamp: Timestamp
    last_day_volume_in_usd: float

    @staticmethod
    def from_get(get_result:Mapping, t: Timestamp):
        return DailyPoolData(
            timestamp=t,
            asset1=int(get_result['asset_1']['id']),
            asset2=int(get_result['asset_2']['id']),
            last_day_volume_in_usd=float(get_result['last_day_volume_in_usd'])
        )


def get_daily_data(limit:Optional[int] = None):

    if limit is None:
        limit_str = ''
    else:
        limit_str = f'limit={limit}&'

    url = f'https://mainnet.analytics.tinyman.org/api/v1/pools/?{limit_str}ordering=-liquidity&with_statistics=true&verified_only=true'

    res = requests.get(url).json()['results']
    t = Timestamp.get()

    return [DailyPoolData.from_get(x,t) for x in res]


class DailyDataLogger(BaseSqliteLogger):

    def __init__(self, dbfile: str):
        super().__init__('dailyData', dbfile)

    # CHECK that reserves are int
    def _table_format(self) -> list[str]:
        return [
            "asset1 int",
            "asset2 int",
            "last_day_volume_in_usd real",
            "now timestamp",
            "utcnow timestamp"
        ]

    def _row_to_tuple(self, row: DailyPoolData) -> tuple:
        return (row.asset1,
                row.asset2,
                row.last_day_volume_in_usd,
                row.timestamp.now,
                row.timestamp.utcnow
                )

