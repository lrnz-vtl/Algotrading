import json
import logging
logging.basicConfig(level=logging.INFO)

from algo.stream.marketstream import MultiPoolStream, log_stream, default_sample_interval, default_log_interval
from algo.stream import sqlite
from tinyman.v1.client import TinymanMainnetClient
import asyncio
import time
import argparse
from algo.universe.assets import Universe
from pathlib import Path

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log market data to file.')
    parser.add_argument('-f', dest='base_folder',  type=str, required=True)
    parser.add_argument( '-s', dest='sample_interval', type=int, default=default_sample_interval, help='Sample interval in seconds')
    parser.add_argument('-l', dest='log_interval', type=int, default=default_log_interval, help='log interval in seconds')
    parser.add_argument('-t', '--test', dest='test', action='store_true',
                        help='Test (quicker) run')

    args = parser.parse_args()

    base_folder = Path(args.base_folder)

    logger = logging.getLogger("MarketLogger")
    logger.setLevel(logging.INFO)

    client = TinymanMainnetClient()

    universe = Universe(client=client, check_pairs=True, test=args.test)

    info = {
        'sample_interval': args.sample_interval,
        'log_interval': args.log_interval,
        'pairs': universe.pairs
    }

    timestr = time.strftime("%Y%m%d-%H%M%S")
    info_fname = base_folder / f'run_{timestr}.json'
    dbfname = base_folder / 'marketData.db'

    with open(info_fname, 'w') as f:
        json.dump(info, f)

    logger.info(f"Logging {len(universe.pairs)} asset pairs")

    multiPoolStream = MultiPoolStream(assetPairs=universe.pairs, client=client, sample_interval=args.sample_interval,
                                      log_interval=args.log_interval, logger=logger)

    with sqlite.MarketSqliteLogger(dbfile=str(dbfname)) as marketLogger:

        marketLogger.create_table(ignore_existing=True)
        def logf(x):
            logger.debug(f'Logging row {x}')
            marketLogger.log(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=None, logger_fun=logf)
        asyncio.run(logger_coroutine)
