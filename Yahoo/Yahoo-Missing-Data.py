import sys
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')
from SP500 import FetchStocksList
import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data returned by tabula.
import traceback # For stacktrace in case of exception. 
import json
import datetime
from pytz import timezone
import yfinance as yf

start_date = datetime.date(2022, 6,30)

end_date = datetime.date(2022,7,1)


day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()


#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
Tickerdata = pd.read_csv("../Tickers/Ticker-List-Yahoo.csv")
ticker_list_splited = Tickerdata["Symbol"].values.tolist()
print("Total Tickers: ", len(ticker_list_splited))
ticker_list = " ".join(ticker_list_splited)

#For testing purpose only.
#ticker_list_splited = ["AAPL", "MOLX"]
#ticker_list = "AAPL MOLX"

start_date_str = str(start_date)
end_date_str = str(end_date)

print(start_date_str)
print(end_date_str)

data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = ticker_list,
        
        start= start_date_str,
        end= end_date_str,
    
        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        #period = "ytd",
        
        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval = "1m",

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by = 'ticker',

        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust = True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost = False,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads = True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy = None
    
    )

df = pd.DataFrame()

for each_ticker in ticker_list_splited:
    df_temp = data[each_ticker]
    df_temp["Ticker"] = each_ticker
    df = df.append(df_temp)
    print(each_ticker)

df = df.reset_index()
if(not "Datetime" in df.columns):
    df = df.rename(columns = {'index' : 'Datetime'})

print(df)
#df = df.reset_index().drop(['index'], axis =1)
df['Date'] = pd.to_datetime(df['Datetime']).dt.date
df['Timestamp'] = pd.to_datetime(df['Datetime']).dt.time
df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y%m%d")
df = df.drop(['Datetime'], axis = 1)
if ("Adj Close" in df.columns):
    df = df.drop(['Adj Close'], axis = 1)
df = df.sort_values(["Ticker","Timestamp"])
df = df.rename(columns= {'Open' : 'OpenPrice', 'High' : 'HighPrice', 'Low' : 'LowPrice', 'Close' : 'ClosePrice', 'Volume' : 'TotalVolume'})
df.to_csv("Yahoo-Data/" + start_date_str.replace("-","") + ".csv", index = False, columns = ["Date", "Timestamp","Ticker","OpenPrice","HighPrice","LowPrice","ClosePrice", "TotalVolume"])