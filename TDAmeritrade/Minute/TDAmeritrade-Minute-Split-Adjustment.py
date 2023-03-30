import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data.
import os
from datetime import datetime as dt
import sys
import json
import datetime
from pytz import timezone
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, update
sys.path.insert(1, '../../Yahoo/')

# Database connection 
database_connection_string = 'postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg'
engine = create_engine(database_connection_string)
meta = MetaData(bind=engine)
MetaData.reflect(meta)
TDAmeritradeMinutePrice = meta.tables['TDAmeritradeMinutePrice']

day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file
f = open('../../configurations.json')
# returns JSON object as a dictionary
configurations = json.load(f)
# Closing file
f.close()

tz = timezone(configurations["TDAmeritrade-Split"]["time_zone"])
# Date from where the split shall be applied. 
#To Apply the split from Today's data. This is used for live data stream.
current_date = dt.now(tz).date()
apply_split_on_date = current_date.strftime('%Y%m%d')
apply_split_on_date_for_db = current_date.strftime('%Y-%m-%d')

#Loading the Split History File
df_split_complete = pd.read_csv("..\..\Yahoo\Splits.csv")
df_split = df_split_complete.rename(columns={"Stock Splits" : "Splits SplitRatio", "Ticker" : "Security Symbol", "Date" : "Splits ExDate"})
df_split["Splits ExDate"] = pd.to_datetime(df_split["Splits ExDate"])
df_split = df_split[df_split["Splits ExDate"] == dt.strptime(apply_split_on_date, "%Y%m%d")]
df_split["Splits ExDate"] = df_split["Splits ExDate"].dt.strftime('%Y%m%d')
df_split = df_split.reset_index(drop=True)

#Create a Dictionary Format for Split History Data
split_data = {}
for i in range(df_split.shape[0]):
    if df_split.loc[i, "Security Symbol"] in split_data:
        already_stored_data = dict(split_data[df_split.loc[i, "Security Symbol"]])
        already_stored_data[df_split.loc[i, "Splits ExDate"]] = df_split.loc[i, "Splits SplitRatio"]
        split_data[df_split.loc[i, "Security Symbol"]] = dict(already_stored_data)
        
    else:
        split_data[df_split.loc[i, "Security Symbol"]] = {df_split.loc[i, "Splits ExDate"] :df_split.loc[i, "Splits SplitRatio"] }

#Logging the entry when the script has run

# Open a file with access mode 'a'
file_object = open('TDAmeritrade-Minute-Split-logfile.txt', 'a')
file_object.write('Last Run: ' + str(datetime.datetime.now()) + "\n\n" + str(split_data) + "\n")
# Close the file
file_object.close()


# If the Split data is empty which means that there is no stock whose split is applied then no need to Apply Split on any file.
if(bool(split_data)):
    for stock in split_data:
        df = pd.read_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" + stock + ".csv")
        print("Stock Split Applied On: ", stock)
        df["Date"] = pd.to_datetime(df["Date"], format = "%Y%m%d")
        
        current_ratio = split_data[stock][apply_split_on_date]
        
        #Segregating Data
        df_before_split = df[df["Date"] < dt.strptime(apply_split_on_date, "%Y%m%d")]
        df_after_split = df[df["Date"] >= dt.strptime(apply_split_on_date, "%Y%m%d")]
        
        #Applying Split
        df_before_split['open'] = df_before_split['open'] / current_ratio
        df_before_split['high'] = df_before_split['high'] / current_ratio
        df_before_split['low'] = df_before_split['low'] / current_ratio
        df_before_split['close'] = df_before_split['close'] / current_ratio
        #Volume remains the same even after the stock split because if price is reduced by 1/2 then the quantity should be doubled.
        #df_before_split['TotalVolume'] = df_before_split['TotalQuantity'] * df_before_split['ClosePrice']
        
        df_resultant = pd.concat([df_before_split, df_after_split], ignore_index=True)
        df_resultant["Date"] = df_resultant["Date"].dt.strftime('%Y%m%d')
        df_resultant.to_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" + stock + ".csv", index = False)
        
        #Getting the Ticker Id for Symbol from Database
        with engine.connect() as con:    
            rows = con.execute(f'SELECT "Id" FROM public."Ticker" where "Symbol" = \'{stock}\'')
            for row in rows:
                ticker_id = row[0]
        
        #Updating the Database
        current_ratio_db = int(current_ratio)
        u = update(TDAmeritradeMinutePrice)
        u = u.values({
        "Open": (TDAmeritradeMinutePrice.c.Open / current_ratio_db), 
        "High": (TDAmeritradeMinutePrice.c.High / current_ratio_db), 
        "Low": (TDAmeritradeMinutePrice.c.Low / current_ratio_db), 
        "Close": (TDAmeritradeMinutePrice.c.Close / current_ratio_db)
        })
        u = u.where(TDAmeritradeMinutePrice.c.TickerId == ticker_id).where(TDAmeritradeMinutePrice.c.Date < apply_split_on_date_for_db)
        engine.execute(u)
        
else:
    print("No Split Data is Found for: ", apply_split_on_date)

