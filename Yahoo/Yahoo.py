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

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 

day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

tz = timezone(configurations["Yahoo"]["time_zone"])
time_to_fetch_data = datetime.time(configurations["Yahoo"]["time_to_fetch_data"][0],configurations["Quandl"]["time_to_fetch_data"][1],configurations["Quandl"]["time_to_fetch_data"][2])
next_fetch_date = datetime.date(configurations["Yahoo"]["next_fetch_date"][0],configurations["Quandl"]["next_fetch_date"][1],configurations["Quandl"]["next_fetch_date"][2])

while True:
    
    today_date = datetime.datetime.now(tz).date()
    print(today_date)
    print(str(today_date))
    # Only Fetch Data Between 1 and 5 Inclusive i.e. Tuesday till Saturday. And also not before 12 pm everyday.
    if((today_date == next_fetch_date) and (datetime.datetime.now(tz).time() >= time_to_fetch_data)):
        
        print("Fetching Data: Date: ", today_date, " Time: ", datetime.datetime.now().time())
        
        #Fetching the Live Stocks List and Updating TickerList.csv
        FetchStocksList()

        #Fetching the Ticker List
        Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
        ticker_list_splited = Tickerdata["Symbol"].values.tolist()
        print("Total Tickers: ", len(ticker_list_splited))

        ticker_list = " ".join(ticker_list_splited)

        if(today_date.weekday() == 5):
            next_fetch_date = next_fetch_date + (3*day_delta)
        else:
            next_fetch_date = next_fetch_date + day_delta

        start_date = today_date - day_delta
        
        end_date = today_date

        for i in range((end_date - start_date).days):
            current_date = start_date + i*day_delta
            current_day = current_date.weekday()
            #data = str(current_date).replace("-","")
            #print("Date: ", data, " Day: ", current_day)
            
            if(current_day < 5):
                start_date_str = str(start_date)
                end_date_str = str(end_date)

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
                        print(each_ticker, "   ", data[each_ticker].shape)
                        df_temp = data[each_ticker]
                        df_temp["Ticker"] = each_ticker
                        df = df.append(df_temp)
                        
                    df = df.reset_index()
                    df = df.sort_values(["Datetime"])
                    df = df.reset_index().drop(['index'], axis =1)
                    df['Date'] = pd.to_datetime(df['Datetime']).dt.date
                    df['Time'] = pd.to_datetime(df['Datetime']).dt.time
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y%m%d")
                    df = df.drop(['Datetime'], axis = 1)
                    df.to_csv("Yahoo-Data/" + start_date_str.replace("-","") + ".csv", index = False)