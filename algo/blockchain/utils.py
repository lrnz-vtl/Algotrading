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
    # We do not want lookahead in the data, so each 5 minute slice should contain the data for the past, not the future
    time_5min = ((time_col // 300) + ((time_col % 300) > 0).astype(int)) * 300
    return pd.to_datetime(time_5min, unit='s', utc=True)
