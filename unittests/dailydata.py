import unittest
import logging
from algo.daily_data.daily_logger import DailyDataLogger, get_daily_data
import time
import uuid
from contextlib import closing
import os


class TestDailyData(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        logging.basicConfig(level=logging.NOTSET)
        self.logger = logging.getLogger("TestDaily")

        filename = kwargs.get('filename')
        if filename:
            fh = logging.FileHandler(filename)
            fh.setLevel(logging.INFO)
            self.logger.addHandler(fh)
        super().__init__(*args, **kwargs)

    def test_data(self):

        dbfname = f'/tmp/{str(uuid.uuid4())}.db'

        with DailyDataLogger(dbfile=dbfname) as dailyLogger:

            dailyLogger.create_table(ignore_existing=True)

            for i in range(3):
                for row in get_daily_data():
                    dailyLogger.log(row)
                time.sleep(5)

            with closing(dailyLogger.con.cursor()) as c:
                c.execute(f"select * from {dailyLogger.tablename}")
                for x in c.fetchall():
                    self.logger.info(x)

        os.remove(dbfname)
