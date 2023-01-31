import requests
import datetime
import datetime as dt
import pandas as pd
from matplotlib import pyplot as plt

# apikey=**my api
# secret=**my api



# historical price data from binance api
symbol = "BTCUSDT"
period = "5m"
limit = "1000"
startDT = dt.datetime(2022, 1, 10, 00, 00, 00)
endDT = dt.datetime(2022, 1, 11, 00, 00, 00)

startTime = int(datetime.datetime.timestamp(startDT)) * 1000
endTime = int(datetime.datetime.timestamp(endDT)) * 1000

print(startTime, endTime)

# url = f'https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period={period}&limit={limit}'
# url = f'https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period={period}&limit={limit}&startTime={startTime}&endTime={endTime}'
url = f'https://www.binance.com/api/v3/klines?symbol={symbol}&interval={period}&limit={limit}&startTime={startTime}&endTime={endTime}'

oi_data = requests.get(url)

oi_json = oi_data.json()

print(len(oi_json))

# df = pd.DataFrame(oi_json).set_index('timestamp')
# df.index = pd.to_datetime(df.index.astype(int), unit='ms')
# df['sumOpenInterest'] = df['sumOpenInterest'].astype(float)
# df['sumOpenInterest'].plot()
# plt.show()

# print(df)

# symbol	STRING	YES
# period	ENUM	YES	"5m","15m","30m","1h","2h","4h","6h","12h","1d"
# limit	LONG	NO	default 30, max 500
# startTime	LONG	NO
# endTime	LONG	NO
