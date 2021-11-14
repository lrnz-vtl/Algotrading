from datetime import datetime
from tinyman.v1.pools import Pool
from utils.timestamp import Timestamp
from datetime import timezone, timedelta
from typing import Optional, Tuple, Generator
import numpy as np


class RunningMean:

    def __init__(self):
        self.x = np.nan
        self.n = 0

    def add(self, x):
        if np.isnan(self.x):
            self.x = 0
        self.x += x
        self.n += 1

    def value(self):
        return self.x / self.n


def time_bucket(utcnow, log_interval):
    time = (utcnow - datetime(1970, 1, 1, tzinfo=timezone.utc))
    delta = timedelta(seconds=time.total_seconds() % log_interval)
    return utcnow - delta


def aggregatePrice(bucket_delta: int = 60 * 5, logger=None) -> Generator[Tuple[datetime, float], Tuple[Timestamp, Pool], None]:
    """
    Very basic time-average price aggregator
    """

    time: Optional[Timestamp] = None
    bucket_delta = bucket_delta
    mean = None

    while True:
        t, pool = (yield)

        # Price of buying infinitesimal amount of asset2 in units of asset1, excluding transaction costs
        price = pool.asset1_reserves / pool.asset2_reserves

        if mean is not None:
            t0 = time_bucket(time.utcnow, bucket_delta)
            t1 = time_bucket(t.utcnow, bucket_delta)

            if t0 != t1:
                yield t0, mean.value()
                if logger is not None:
                    logger.debug(f"Number of samples: {mean.n}")
                mean = RunningMean()
        else:
            mean = RunningMean()

        mean.add(price)
        time = t
