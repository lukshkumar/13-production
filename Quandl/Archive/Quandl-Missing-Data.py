import numpy as np # For Mathematical computations.
import pandas as pd # For Data processing and Operations on data returned by tabula.
import requests # To Request APIs (OCR-Space)
import traceback # For stacktrace in case of exception. 
import zipfile, io
import json
import datetime
from pytz import timezone
import sys
sys.path.insert(1,'../')

#Quandl data is updated at 11 am EST time Tuesday to Saturday so we fetch data at 12 pm EST everyday to make sure that it has all the data we need. 

day_delta = datetime.timedelta(days=1)

# Start and End Time for Few missing Days.
start_date = datetime.date(2022,1,21)
end_date = datetime.date(2022,1,23)

#YYYY_MM_DD - Format

#1 Minute Bars for Specific Day
#data = "20210802"

#Ticker list for Specific Day
#data = "20151117-master"

#All Data for all Day
#data = "all-data"

#List of All Trading Days
#data = "all-trading-days"

#Reading the Configurations from JSON file

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

api_key = configurations["Quandl"]["api_key"]

print("The Quandl-Missing-Data Program is Runing.......")

for i in range((end_date - start_date).days):
    current_date = start_date + i*day_delta
    current_day = current_date.weekday()
    data = str(current_date).replace("-","")
    print("Date: ", data, " Day: ", current_day)
    
    if(current_day < 5):

        endpoint ="https://www.quandl.com/api/v3/databases/AS500/download?api_key=" + api_key + "&download_type=" + data

        try:
            r = requests.get(endpoint)
            
            #Save as Zip File
            with open("Quandl-Data-Split-Applied/Data.zip", "wb") as code:
                code.write(r.content)
            
            #Extract Zip File
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(r"Quandl-Data-Split-Applied/")
            
            print("INFO: Data Fetched and Extracted Successfully!")
            
        except:
            
            if("quandl_error" in json.loads(r.content)):
                print(r.content)
                print("Error: Data is not available for specified Day on Quandl Server!")
            
            print("ERROR: Quandl REQUEST EXCEPTION")
            error_message = traceback.format_exc()
            print(error_message)