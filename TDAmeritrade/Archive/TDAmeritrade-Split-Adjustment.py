import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data.
import os
from datetime import datetime as dt
import sys
import json
import datetime
from pytz import timezone
sys.path.insert(1, '../Yahoo/')
from Split import FetchSplitsList

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 
#Therefore we apply the stock split at 2 pm EST everyday to make sure we have fetched the Quandl file completely. 

day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file
f = open('../configurations.json')
# returns JSON object as a dictionary
configurations = json.load(f)
# Closing file
f.close()

tz = timezone(configurations["TDAmeritrade-Split"]["time_zone"])
time_to_fetch_data = datetime.time(configurations["TDAmeritrade-Split"]["time_to_fetch_data"][0],configurations["TDAmeritrade-Split"]["time_to_fetch_data"][1],configurations["TDAmeritrade-Split"]["time_to_fetch_data"][2])
next_fetch_date = datetime.date(configurations["TDAmeritrade-Split"]["next_fetch_date"][0],configurations["TDAmeritrade-Split"]["next_fetch_date"][1],configurations["TDAmeritrade-Split"]["next_fetch_date"][2])

print("The TDAmeritrade-Split-Adjustment Program is Runing.......")

while True:
    
    today_date = dt.now(tz).date()
    
    # Only Fetch Data Between 1 and 5 Inclusive i.e. Tuesday till Saturday. And also not before 12 pm everyday.
    if((today_date == next_fetch_date) and (dt.now(tz).time() >= time_to_fetch_data)):
        
        print("Fetching Data: Date: ", today_date, " Time: ", dt.now().time())
        
        if(today_date.weekday() == 5):
            next_fetch_date = next_fetch_date + (3*day_delta)
        else:
            next_fetch_date = next_fetch_date + day_delta

        # Date from where the split shall be applied. 

        #To Apply the split from Today's data. This is used for live data stream.
        apply_split_after_date = (dt.now(tz).date() - datetime.timedelta(days=1)).strftime('%Y%m%d')

        #Generating the real-time Split History Data
        FetchSplitsList()

        #Loading the Split History File
        df_split_complete = pd.read_csv("..\Yahoo\Splits.csv")
        df_split = df_split_complete.rename(columns={"Stock Splits" : "Splits SplitRatio", "Ticker" : "Security Symbol", "Date" : "Splits ExDate"})
        df_split["Splits ExDate"] = pd.to_datetime(df_split["Splits ExDate"])
        df_split = df_split[df_split["Splits ExDate"] == dt.strptime(apply_split_after_date, "%Y%m%d")]
        df_split["Splits ExDate"] = df_split["Splits ExDate"].dt.strftime('%Y%m%d')
        df_split =df_split.reset_index(drop=True)

        print(df_split)

        #Create a Dictionary Format for Split History Data

        split_data = {}

        for i in range(df_split.shape[0]):
            if df_split.loc[i, "Security Symbol"] in split_data:
                already_stored_data = dict(split_data[df_split.loc[i, "Security Symbol"]])
                already_stored_data[df_split.loc[i, "Splits ExDate"]] = df_split.loc[i, "Splits SplitRatio"]
                split_data[df_split.loc[i, "Security Symbol"]] = dict(already_stored_data)
                
            else:
                split_data[df_split.loc[i, "Security Symbol"]] = {df_split.loc[i, "Splits ExDate"] :df_split.loc[i, "Splits SplitRatio"] }

        #Multiplying the Ratio in case of multiple splits.

        for stock in split_data:
            ratio = 1
            for each_split in split_data[stock]:
                ratio = ratio * split_data[stock][each_split]
                split_data[stock][each_split] = ratio

        print(split_data)

            
        #Apply Split Function

        # Latest Split Date - 08/02/2021
        # Read all files from 2 Jan 2008 till date. Apply Split on each file per day. 
        entries = os.listdir('TDAmeritrade-Data-By-Day-Split-Applied/')

        #Sort all files by date
        entries.sort(reverse = True)

        #Remove Extra files
        #entries.remove("Data.zip")

        # If the Split data is empty which means that there is no stock whose split is applied then no need to Apply Split on any file.
        if(bool(split_data)):
            for entry in entries:
                print("Filename: ", entry)
                df = pd.read_csv("TDAmeritrade-Data-By-Day-Split-Applied/" + entry)
                current_file_date = dt.strptime(entry.replace(".csv", ""), "%Y%m%d")
                for stock in split_data:
                    current_ratio = -1
                    for each_split_date in split_data[stock]:
                        if(current_file_date < dt.strptime(each_split_date, "%Y%m%d")):
                            current_ratio = split_data[stock][each_split_date]
                    if(current_ratio != -1):
                        print("Stock Split Applied On: ", stock)
                        df['open'] = np.where(df['Ticker'] == stock, df['open'] / current_ratio, df['open'])
                        df['high'] = np.where(df['Ticker'] == stock, df['high'] / current_ratio, df['high'])
                        df['low'] = np.where(df['Ticker'] == stock, df['low'] / current_ratio, df['low'])
                        df['close'] = np.where(df['Ticker'] == stock, df['close'] / current_ratio, df['close'])
                        #df['TotalQuantity'] = np.where(df['Ticker'] == stock, df['TotalQuantity'] * current_ratio, df['TotalQuantity'])
                        #df['TotalVolume'] = np.where(df['Ticker'] == stock, df['TotalQuantity'] * df['ClosePrice'], df['TotalVolume'])
                        
                df.to_csv("TDAmeritrade-Data-By-Day-Split-Applied/" + entry, index = False)
        else:
            print("No Split Data is Found for: ", apply_split_after_date)
