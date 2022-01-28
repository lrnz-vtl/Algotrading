from stream.sqlite import MarketSqliteLogger
from contextlib import closing
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Read logs from sqlite file.')
    parser.add_argument('dbfname', '-f', type=str, required=True)

    args = parser.parse_args()

    dbfname = args.dbfname

    with MarketSqliteLogger(dbfile=dbfname) as marketLogger:
        with closing(marketLogger.con.cursor()) as c:
            c.execute(f"select * from {marketLogger.tablename}")
            for x in c.fetchall():
                print(x)
