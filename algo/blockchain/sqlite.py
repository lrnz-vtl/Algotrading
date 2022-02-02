from algo.sqlite.base import BaseSqliteLogger
import os
from definitions import ROOT_DIR
from algo.blockchain.info import BlockInfo

CHAINDATA_BASEFOLDER = os.path.join(ROOT_DIR, 'chainData')


class ChainDataLogger(BaseSqliteLogger):

    def __init__(self, cache_name: str):
        db_fname = os.path.join(CHAINDATA_BASEFOLDER, cache_name, 'data.db')
        super().__init__('chainData', db_fname)

    def _table_format(self) -> list[tuple[str,str]]:
        return [
            ("block", "int"),
            ("timestamp", "timestamp")
        ]

    def _row_to_tuple(self, row: BlockInfo) -> tuple:
        return row.block, row.timestamp