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

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 

#Reading the Configurations from JSON file

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()


#Fetching the Live Stocks List and Updating TickerList.csv
FetchStocksList()

#Fetching the Ticker List
Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
ticker_list_splited = Tickerdata["Symbol"].values.tolist()
print("Total Tickers: ", len(ticker_list_splited))

#ticker_list_splited = ["BF.B"]

count = 1

print("------------------ Extracting Ticker Split and Dividend Data ------------------")
for each_ticker in ticker_list_splited:

    print("Ticker Number: ", count)
    data = yf.Ticker(each_ticker)
    
    try:
        # get historical splits data
        splits = data.splits.reset_index()

        # show dividends
        dividends = data.dividends.reset_index()

        splits = splits.append(dividends).sort_values(['Date'])


        splits.to_csv("Split-And-Dividends-Data/" + each_ticker + ".csv", index = False)

        count += 1
    
    except:
        count += 1

