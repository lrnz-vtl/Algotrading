import json
import logging
import os.path

logging.basicConfig(level=logging.INFO)
from algo.stream.marketstream import MultiPoolStream, log_stream, default_sample_interval, default_log_interval, MARKETLOG_BASEFOLDER
from algo.stream import sqlite
from tinyman.v1.client import TinymanMainnetClient
import asyncio
import time
import argparse
from algo.universe.universe import Universe

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log market data to file.')
    parser.add_argument('-c', dest='universe_cache_name',  type=str, required=True, help='Name of the universe cache to log')
    # parser.add_argument('-r', dest='run_name', type=str, required=True, help='Name of the logging run')
    parser.add_argument('-s', dest='sample_interval', type=int, default=default_sample_interval, help='Sample interval in seconds')
    parser.add_argument('-l', dest='log_interval', type=int, default=default_log_interval, help='log interval in seconds')

    args = parser.parse_args()

    universe = Universe.from_cache(args.universe_cache_name)

    timestr = time.strftime("%Y%m%d-%H%M%S")
    run_name = timestr
    run_basefolder = os.path.join(MARKETLOG_BASEFOLDER, run_name)
    os.makedirs(run_basefolder, exist_ok=True)

    logger = logging.getLogger("MarketLogger")
    logger.setLevel(logging.INFO)

    client = TinymanMainnetClient()

    info = {
        'sample_interval': args.sample_interval,
        'log_interval': args.log_interval,
        'universe_cache_name': args.universe_cache_name
    }

    dbfname = os.path.join(run_basefolder, 'data.db')
    infofname = os.path.join(run_basefolder, 'info.json')

    with open(infofname, 'w') as f:
        json.dump(info, f, indent=4)

    pairs = [(pool.asset1_id, pool.asset2_id) for pool in universe.pools]
    logger.info(f"Logging {len(pairs)} asset pairs")

    multiPoolStream = MultiPoolStream(assetPairs=pairs, client=client, sample_interval=args.sample_interval,
                                      log_interval=args.log_interval, logger=logger)

    with sqlite.MarketSqliteLogger(run_name=run_name) as marketLogger:

        marketLogger.create_table(ignore_existing=True)
        def logf(x):
            logger.debug(f'Logging row {x}')
            marketLogger.log(x)

        logger_coroutine = log_stream(multiPoolStream.run(), timeout=None, logger_fun=logf)
        asyncio.run(logger_coroutine)
