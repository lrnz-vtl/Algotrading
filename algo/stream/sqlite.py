from algo.stream import marketstream
from algo.sqlite.base import BaseSqliteLogger
import arrow


def convert_arrowdatetime(s):
    return arrow.get(s)


def adapt_arrowdatetime(adt):
    return adt.isoformat()


class MarketSqliteLogger(BaseSqliteLogger):

    def __init__(self, dbfile: str):
        super().__init__('marketData', dbfile)

    # CHECK that reserves are int
    def _table_format(self) -> list[str]:
        return [
            "asset1 int",
            "asset2 int",
            "asset1_reserves int",
            "asset2_reserves int",
            "price real",
            "now timestamp",
            "utcnow timestamp"
        ]

    def _row_to_tuple(self, row: marketstream.Row) -> tuple:
        return (row.asset1,
                row.asset2,
                row.asset1_reserves,
                row.asset2_reserves,
                row.price,
                row.timestamp.now,
                row.timestamp.utcnow
                )
