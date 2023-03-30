import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data.
import os
from datetime import datetime as dt
import sys
import json
import datetime
from pytz import timezone
sys.path.insert(1, '../../Yahoo/')

day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file
f = open('../../configurations.json')
# returns JSON object as a dictionary
configurations = json.load(f)
# Closing file
f.close()

tz = timezone(configurations["TDAmeritrade-Split"]["time_zone"])
# Date from where the split shall be applied. 
# To Apply the split on missing data, we supply the user defined date from where the splits should be applied. In case we want to apply the split on entire data 
# then we will provide the starting day of data i.e. the first entry in entire data.
apply_split_on_date = "20080101"

#Loading the Split History File
df_split_complete = pd.read_csv("..\..\Yahoo\Splits.csv")
df_split = df_split_complete.rename(columns={"Stock Splits" : "Splits SplitRatio", "Ticker" : "Security Symbol", "Date" : "Splits ExDate"})
df_split["Splits ExDate"] = pd.to_datetime(df_split["Splits ExDate"])
df_split = df_split[df_split["Splits ExDate"] >= dt.strptime(apply_split_on_date, "%Y%m%d")]
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

# If the Split data is empty which means that there is no stock whose split is applied then no need to Apply Split on any file.
if(bool(split_data)):
    for stock in split_data:

        df = pd.read_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" + stock + ".csv")
        print("Stock Split Applied On: ", stock)
        df["Date"] = pd.to_datetime(df["Date"])
            
        #This returns a list of Tuples having dictionary key at 0 index and dictionary value at 1st index. We are traversing the list in reverse order and applying 
        #split from the start to the latest.
        stock_date_and_ratio_data = list(split_data[stock].items())

        for each_split_index in range(len(stock_date_and_ratio_data) - 1, -1, -1):
            date_on_which_split_happened = stock_date_and_ratio_data[each_split_index][0]
            current_ratio = stock_date_and_ratio_data[each_split_index][1]
            
            #Segregating Data
            df_before_split = df[df["Date"] < dt.strptime(date_on_which_split_happened, "%Y%m%d")]
            df_after_split = df[df["Date"] >= dt.strptime(date_on_which_split_happened, "%Y%m%d")]
            
            #Applying Split
            df_before_split['open'] = df_before_split['open'] / current_ratio
            df_before_split['high'] = df_before_split['high'] / current_ratio
            df_before_split['low'] = df_before_split['low'] / current_ratio
            df_before_split['close'] = df_before_split['close'] / current_ratio
            #df_before_split['TotalVolume'] = df_before_split['TotalQuantity'] * df_before_split['ClosePrice']
                
            df = df_before_split.append(df_after_split)
        
        df.to_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" +  + stock + ".csv", index = False)
else:
    print("No Split Data is Found for: ", apply_split_on_date)