from datetime import datetime
from tinyman.v1.pools import Pool
from tools.timestamp import Timestamp
from datetime import timezone, timedelta
from typing import Optional, Tuple, Generator
import numpy as np
from dataclasses import dataclass


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


def time_bucket(timestamp: Timestamp, log_interval):
    time = (timestamp.utcnow - datetime(1970, 1, 1, tzinfo=timezone.utc))
    delta = timedelta(seconds=time.total_seconds() % log_interval)
    return Timestamp(utcnow=timestamp.utcnow - delta, now=timestamp.now - delta)


@dataclass
class AveragePrice:
    timestamp: Timestamp
    price: float


def aggregatePrice(bucket_delta: int = 60 * 5, logger=None) -> Generator[AveragePrice, Tuple[Timestamp, Pool], None]:
    """
    Very basic time-average price aggregator
    """

    time: Optional[Timestamp] = None
    bucket_delta = bucket_delta
    mean = None

    while True:
        t, pool = (yield)
        t: Timestamp
        pool: Pool

        # Price of buying infinitesimal amount of asset2 in units of asset1, excluding transaction costs
        price = pool.asset1_reserves / pool.asset2_reserves

        if mean is not None:
            t0 = time_bucket(time, bucket_delta)
            t1 = time_bucket(t, bucket_delta)

            if t0.utcnow != t1.utcnow:
                yield AveragePrice(timestamp=t0, price=mean.value())
                if logger is not None:
                    logger.debug(f"Number of samples: {mean.n}")
                mean = RunningMean()
        else:
            mean = RunningMean()

        mean.add(price)
        time = t
