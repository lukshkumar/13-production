import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data returned by tabula.
import requests # To Request APIs (OCR-Space)
import traceback # For stacktrace in case of exception. 
import zipfile, io
import json
import datetime
from pytz import timezone
import sys
import os
sys.path.insert(1,'../')

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 
day_delta = datetime.timedelta(days=1)

#Reading the Configurations from JSON file
f = open('../configurations.json')
# returns JSON object as a dictionary
configurations = json.load(f)
# Closing file
f.close()

tz = timezone(configurations["Quandl"]["time_zone"])
api_key = configurations["Quandl"]["api_key"]

print("The Quandl Program is Runing.......")
    
today_date = datetime.datetime.now(tz).date()
start_date = today_date - day_delta
date_to_fetch_data = str(start_date).replace("-","")
print("Date: ", date_to_fetch_data)

endpoint ="https://www.quandl.com/api/v3/databases/AS500/download?api_key=" + api_key + "&download_type=" + date_to_fetch_data

try:
    # ------------- Fetching the Quandl Data --------------
    r = requests.get(endpoint)
    
    #Save as Zip File
    with open("Quandl-Minute-Data-By-Day/Data.zip", "wb") as code:
        code.write(r.content)
    
    #Extract Zip File
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(r"Quandl-Minute-Data-By-Day/")
    
    print("INFO: Data Fetched and Extracted Successfully!")

    # ----------------- Code to convert data files by date to data files by Individual Tickers -----------------
    
    files_in_data_by_ticker_folder = os.listdir('Quandl-Minute-Data-By-Ticker-Split-Applied/')
    df_today_file = pd.read_csv("Quandl-Minute-Data-By-Day/" + date_to_fetch_data + ".csv")
    df_today_file = df_today_file.groupby('Ticker')
    for group_name, group_df in df_today_file:
        ticker_csv_filename = group_name + ".csv"
        if(ticker_csv_filename in files_in_data_by_ticker_folder):
            group_df.to_csv("Quandl-Minute-Data-By-Ticker-Split-Applied/" + ticker_csv_filename, mode = 'a', index = False, header = False)
            print("File Appended For: ", group_name, " : With Rows Count: ", str(group_df.shape[0]))
        else:
            group_df.to_csv("Quandl-Minute-Data-By-Ticker-Split-Applied/" + ticker_csv_filename, index = False)
            print("File Created For: ", group_name, " : With Rows Count: ", str(group_df.shape[0]))
    
except:
    
    if("quandl_error" in json.loads(r.content)):
        print("Error: Data is not available for specified Day on Quandl Server!")
    
    print("ERROR: Quandl REQUEST EXCEPTION")
    error_message = traceback.format_exc()
    print(error_message)



# Quandl API Information

"""
Start and End Time for Few missing Days.
start_date = datetime.date(2021,7,10)
end_date = datetime.date(2021,9,18)

YYYY_MM_DD - Format

1 Minute Bars for Specific Day
data = "20210802"

Ticker list for Specific Day
data = "20151117-master"

All Data for all Day
data = "all-data"

List of All Trading Days
data = "all-trading-days"

"""