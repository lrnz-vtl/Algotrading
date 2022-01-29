import logging

logging.basicConfig(level=logging.INFO)

from algo.daily_data.daily_logger import DailyDataLogger, get_daily_data
import time
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Log daily market data to file.')
    parser.add_argument('-f', dest='dbfname', type=str, required=True)
    parser.add_argument('-l', dest='log_interval_minutes', type=int, default=30, help='log interval in minutes')

    args = parser.parse_args()

    dbfname = args.dbfname

    logger = logging.getLogger("DailyLogger")
    logger.setLevel(logging.INFO)

    with DailyDataLogger(dbfile=dbfname) as dailyLogger:

        dailyLogger.create_table(ignore_existing=True)

        while True:
            for row in get_daily_data():
                dailyLogger.log(row)
            time.sleep(args.log_interval_minutes * 60)
