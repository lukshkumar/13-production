import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data.
import os
from datetime import datetime as dt
import sys
import json
import datetime
from pytz import timezone
sys.path.insert(1, '../../Yahoo/')

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 
#Therefore we apply the stock split at 2 pm EST everyday to make sure we have fetched the Quandl file completely. 

day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file
f = open('../../configurations.json')
# returns JSON object as a dictionary
configurations = json.load(f)
# Closing file
f.close()

tz = timezone(configurations["Quandl-Split"]["time_zone"])
# Date from where the split shall be applied. 
#To Apply the split from Today's data. This is used for live data stream.
apply_split_on_date = (dt.now(tz).date() - datetime.timedelta(days=1)).strftime('%Y%m%d')

#Loading the Split History File
df_split_complete = pd.read_csv("..\..\Yahoo\Splits.csv")
df_split = df_split_complete.rename(columns={"Stock Splits" : "Splits SplitRatio", "Ticker" : "Security Symbol", "Date" : "Splits ExDate"})
df_split["Splits ExDate"] = pd.to_datetime(df_split["Splits ExDate"])
df_split = df_split[df_split["Splits ExDate"] == dt.strptime(apply_split_on_date, "%Y%m%d")]
df_split["Splits ExDate"] = df_split["Splits ExDate"].dt.strftime('%Y%m%d')
df_split = df_split.reset_index(drop=True)
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

print(split_data)

# Read all files from 2 Jan 2008 till date. Apply Split on each file per day. 
# If the Split data is empty which means that there is no stock whose split is applied then no need to Apply Split on any file.
if(bool(split_data)):
    for stock in split_data:
        df = pd.read_csv("Quandl-Minute-Data-By-Ticker-Split-Applied/" + stock + ".csv")
        print("Stock Split Applied On: ", stock)
        df["Date"] = pd.to_datetime(df["Date"])
        
        current_ratio = split_data[stock][apply_split_on_date]

        #Segregating Data
        df_before_split = df[df["Date"] < dt.strptime(apply_split_on_date, "%Y%m%d")]
        df_after_split = df[df["Date"] >= dt.strptime(apply_split_on_date, "%Y%m%d")]
        
        #Applying Split
        df_before_split['OpenPrice'] = df_before_split['OpenPrice'] / current_ratio
        df_before_split['HighPrice'] =  df_before_split['HighPrice'] / current_ratio
        df_before_split['LowPrice'] = df_before_split['LowPrice'] / current_ratio
        df_before_split['ClosePrice'] = df_before_split['ClosePrice'] / current_ratio
        df_before_split['TotalQuantity'] = df_before_split['TotalQuantity'] * current_ratio
        df_before_split['TotalVolume'] = df_before_split['TotalQuantity'] * df_before_split['ClosePrice']
        
        df_resultant = df_before_split.append(df_after_split)
        df_resultant.to_csv("Quandl-Minute-Data-By-Ticker-Split-Applied/" +  + stock + ".csv", index = False)
else:
    print("No Split Data is Found for: ", apply_split_on_date)

