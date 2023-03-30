import json
import pandas as pd
import datetime  
import os
import zipfile
import sys
import datetime
from pytz import timezone
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')

# The size of each step in days
day_delta = datetime.timedelta(days=1)
f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

new_folder_path = "Data/new/"
extracted_file_path = "Data/Extracted-Data/"
dir_list = os.listdir(new_folder_path)

for file in dir_list:
    if(file.endswith(".csv")):

        csv_filename_original = file
        csv_filename = file.replace("UnderlyingOptionsEODCalcs_", "").replace("-", "")
        
        print(csv_filename)
        
        #Only rename file when it hasn't already been renamed.
        if not os.path.isfile(extracted_file_path + csv_filename):
            #Renaming the extracted csv file so to only have the date in csv filename rather than everything.
            os.rename(new_folder_path + csv_filename_original, extracted_file_path + csv_filename)

        df = pd.read_csv(extracted_file_path + csv_filename)
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

        df.to_csv(extracted_file_path + csv_filename, index = False)
