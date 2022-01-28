import logging
from algo.stream.marketstream import MultiPoolStream, log_stream, default_sample_interval, default_log_interval
from algo.stream import sqlite
from tinyman.v1.client import TinymanMainnetClient
import asyncio
import argparse
from assets import find_pools

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log market data to file.')
    parser.add_argument('-f',dest='dbfname',  type=str, required=True)
    parser.add_argument( '-s',dest='sample_interval', type=int, default=default_sample_interval, help='Sample interval in seconds')
    parser.add_argument( '-l',dest='log_interval', type=int, default=default_log_interval, help='log interval in seconds')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    dbfname = args.dbfname

    logger = logging.getLogger("MarketLogger")
    logger.setLevel(logging.INFO)

    logger.info(f"Fetching pools...")
    assetPairs = find_pools()

    sortedPairs = [(min(x), max(x)) for x in assetPairs]
    # assert len(set(sortedPairs)) == len(sortedPairs), \
    #     f"Len(set(assetPairs)) = {len(set(sortedPairs))}, Len(assetPairs) = {len(sortedPairs)}"
    sortedPairs = list(set(sortedPairs))

    client = TinymanMainnetClient()

    logger.info(f"Logging {len(sortedPairs)} asset pairs")

    multiPoolStream = MultiPoolStream(assetPairs=sortedPairs, client=client, sample_interval=args.sample_interval,
                                      log_interval=args.log_interval, logger=logger)

    with sqlite.MarketSqliteLogger(dbfile=dbfname) as marketLogger:

        marketLogger.create_table(ignore_existing=True)
        def logf(x):
            logger.info(f'Logging row {x}')
            marketLogger.log(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=None, logger_fun=logf)
        asyncio.run(logger_coroutine)
