import sys
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')
from SP500 import FetchStocksList
import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data returned by tabula.
import traceback # For stacktrace in case of exception. 
import json
import datetime
import requests as r
from pytz import timezone

def FetchSplitsList():
    #Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 

    #Fetching the Live Stocks List and Updating TickerList.csv
    #FetchStocksList()

    #Fetching the Ticker List
    Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
    ticker_list_splited = Tickerdata["Symbol"].values.tolist()
    print("Total Tickers: ", len(ticker_list_splited))

    #ticker_list_splited = ["WRB"]

    print("------------------ Extracting Ticker Split Data from Financial Modeling ------------------")
    day_delta = datetime.timedelta(days=30)
    
    #Automatic Date 
    from_date = str(datetime.datetime.now().date() - day_delta)
    print("From Date: ", from_date)
    # Custom Date
    #from_date = "2022-3-01"
    
    json_data = r.get("https://financialmodelingprep.com/api/v3/stock_split_calendar?from=" + from_date + "&apikey=aa8b4631b2ad1b2704741d1a2d6a2611")
    data =  json.loads(json_data.content)
    df = pd.json_normalize(data)
    df =  df.rename(columns = {"date" : "Date"})
    df["Stock Splits"] = 0.0
    df["Ticker"] = df["symbol"]
    
    for i in range(df.shape[0]):
        df.loc[i, "Stock Splits"] = df.loc[i, "numerator"] / df.loc[i, "denominator"]
    
    df = df.drop(columns=["label", "symbol", "numerator", "denominator"])

    trimmed_df = pd.DataFrame(columns=["Date", "Stock Splits", "Ticker"])

    for i in range(df.shape[0]):
        if (df.loc[i, "Ticker"] in ticker_list_splited):
            trimmed_df.loc[len(trimmed_df)] = df.iloc[i,:]
            
    print(trimmed_df)
    trimmed_df.to_csv("../Yahoo/Splits.csv", index = False)

FetchSplitsList()