import urllib.request
import json
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
import datetime  
import numpy as np # For Mathematical computations.
import os
import zipfile, io
from datetime import datetime as dt
import sys
import datetime
from pytz import timezone
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')
from SP500 import FetchStocksList

# The size of each step in days
day_delta = datetime.timedelta(days=1)
f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

tz = timezone(configurations["WinSCP"]["time_zone"])
time_to_fetch_data = datetime.time(configurations["WinSCP"]["time_to_fetch_data"][0],configurations["WinSCP"]["time_to_fetch_data"][1],configurations["WinSCP"]["time_to_fetch_data"][2])
next_fetch_date = datetime.date(configurations["WinSCP"]["next_fetch_date"][0],configurations["WinSCP"]["next_fetch_date"][1],configurations["WinSCP"]["next_fetch_date"][2])

print("The WinSCP Program is Runing.......")

while True:
    
    today_date = datetime.datetime.now(tz).date()
    
    if((today_date == next_fetch_date) and (datetime.datetime.now(tz).time() >= time_to_fetch_data)):
        
        print("Fetching Data: Date: ", today_date, " Time: ", datetime.datetime.now().time())
        
        if(today_date.weekday() == 5):
            next_fetch_date = next_fetch_date + (3*day_delta)
        else:
            next_fetch_date = next_fetch_date + day_delta
        
        print("The Next Call Will be hit on: ", next_fetch_date)

        processing_date = today_date - day_delta
        processing_year = str(processing_date.year)
        processing_month = str(processing_date.month)
        processing_day = str(processing_date.day)

        if(len(processing_day) == 1):
            processing_day = "0" + processing_day
        if(len(processing_month) == 1):
            processing_month = "0" + processing_month

        #Fetching the file from WinSCP SFTP Server.
        os.system('winscp.com /command "option batch abort" "option confirm off" "open sftp://mordecai_projecthamburg_com:Stockchartx!00@sftp.datashop.livevol.com -hostkey=""ssh-rsa 2048 9f:58:fd:a0:04:55:54:0a:f4:b9:78:75:f3:f3:6a:92""" "synchronize local -filemask=""*>=1D"" D:\StockChartX-PROD\WinSCP\Data\Zip-Data /subscriptions/order_000024948/item_000029903" "pause"')

        # datetime(year, month, day, hour, minute, second, microsecond)
        file_date = processing_year + "-" + processing_month + "-" + processing_day
        zip_filename = "UnderlyingOptionsEODCalcs_" + file_date + ".zip"
        csv_filename = "UnderlyingOptionsEODCalcs_" + file_date + ".csv"

        #Extract Zip File
        z = zipfile.ZipFile("Data/Zip-Data/" + zip_filename)
        z.extractall(r"Data/Extracted-Data/")

        df = pd.read_csv("Data/Extracted-Data/" + csv_filename)
        df = df.rename(columns={"underlying_symbol" : "Ticker", 
        "quote_date" : "date", 
        "trade_volume" : "TotalVolume",
        "bid_size_1545" : "bid_size",
        "bid_1545" : "bid",
        "ask_size_1545" : "ask_size",
        "ask_1545" : "ask",
        "underlying_bid_1545" : "underlying_bid",
        "underlying_ask_1545" : "underlying_ask",
        "implied_underlying_price_1545" : "implied_underlying_price",
        "active_underlying_price_1545" : "active_underlying_price",
        "implied_volatility_1545" : "implied_volatility",
        "delta_1545" : "delta",
        "gamma_1545" : "gamma",
        "theta_1545" : "theta",
        "vega_1545" : "vega",
        "rho_1545" : "rho",
        "open" : "OpenPrice",
        "high" : "HighPrice",
        "low" : "LowPrice",
        "close" : "ClosePrice",
        })

        df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y%m%d')
        df["expiration"] = pd.to_datetime(df["expiration"]).dt.strftime('%Y%m%d')

        for i in range(df.shape[0]):

            df.loc[i, 'Ticker'] = "." + str(df.loc[i, 'root']) + str(df.loc[i, 'expiration']) + str(df.loc[i, 'option_type']) + str(int(df.loc[i, 'strike']))

        df.to_csv("Data/Extracted-Data/" + csv_filename, index = False)



