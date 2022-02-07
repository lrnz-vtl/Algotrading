import pandas as pd
import datetime


def datetime_to_int(t: datetime.datetime):
    return int(t.timestamp())


def generator_to_df(gen, time_columns=('time',)):
    df = pd.DataFrame(gen)
    if df.empty:
        print("DataFrame is empty")
    else:
        for col in time_columns:
            df[col] = pd.to_datetime(df[col], unit='s', utc=True)
    return df


def timestamp_to_5min(time_col: pd.Series):
    time_5min = (time_col // 300) * 300
    return pd.to_datetime(time_5min, unit='s', utc=True)
