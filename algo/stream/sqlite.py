import sqlite3
from algo.stream import marketstream
from contextlib import closing
import arrow


def convert_arrowdatetime(s):
    return arrow.get(s)


def adapt_arrowdatetime(adt):
    return adt.isoformat()


class MarketSqliteLogger:

    def __init__(self, dbfile: str):
        self.tablename = 'marketData'
        self.dbfile = dbfile

    def create_table(self, ignore_existing: bool = False):

        with closing(self.con.cursor()) as c:
            c.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{self.tablename}' ''')

            # if the count is 1, then table exists
            if c.fetchone()[0] == 1 and ignore_existing:
                pass
            else:
                # CHECK that reserves are int
                c.execute(
                    f"""
                    create table {self.tablename} 
                    (
                    id INTEGER PRIMARY KEY,
                    asset1 int,
                    asset2 int,
                    asset1_reserves int,
                    asset2_reserves int,
                    price real,
                    now timestamp,
                    utcnow timestamp
                    )
                    """)
        self.con.commit()

    def __enter__(self):
        self.con = sqlite3.connect(self.dbfile)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    def log(self, row: marketstream.Row):
        with closing(self.con.cursor()) as c:
            c.execute(f"""insert into {self.tablename} values 
                        (NULL, ?, ?, ?, ?, ?, ?, ?)""",
                      (row.asset1,
                       row.asset2,
                       row.asset1_reserves,
                       row.asset2_reserves,
                       row.price,
                       row.timestamp.now,
                       row.timestamp.utcnow)
                      )

        self.con.commit()
