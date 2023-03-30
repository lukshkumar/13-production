import urllib.request
import json
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
import datetime  
import numpy as np # For Mathematical computations.
import os
from datetime import datetime as dt
import sys
import datetime
from pytz import timezone
sys.path.insert(1, '../Tickers/')

# variable for request of opening the URL
datafile = urllib.request.URLopener()


# The size of each step in days
day_delta = datetime.timedelta(days=1)
minute_delta = datetime.timedelta(minutes=1)

# datetime(year, month, day, hour, minute, second, microsecond)
start_date = [2022,5,18]
end_date = [2022,7,3]

start_day = datetime.date(start_date[0], start_date[1], start_date[2])
end_day = datetime.date(end_date[0],end_date[1], end_date[2])

entries = os.listdir('Data-By-Ticker/')

#Sort all files by name
entries.sort()

#Remove Extra files
#entries.remove(".ipynb_checkpoints")


for i in range((end_day - start_day).days):
    
    df_current_date = pd.DataFrame()
    current_date_in_date_format = start_day + i*day_delta
    current_day_weekday = current_date_in_date_format.weekday()
    current_date = int(str(current_date_in_date_format).replace("-",""))
    print(current_date)

    if(current_day_weekday < 5):
        
        for entry in entries:
            df = pd.read_csv("Data-By-Ticker/" + entry)
            particular_rows = df.loc[df['datetime'] == current_date, :]
            particular_rows.insert(loc=0, column='Ticker', value=entry.replace(".csv", ""))
            particular_rows.insert(loc=1, column='Time', value="")

            if(particular_rows.shape[0] > 0):
                #Adding Time Column in TDAmeritrade Minute Data.
                datetime_for_particular_entry = datetime.datetime(1,1,1,9,30,0)
                for particular_row_index in range(len(particular_rows)):
                    time_for_particular_entry = datetime_for_particular_entry.time()
                    particular_rows.iloc[particular_row_index, 1] = time_for_particular_entry
                    datetime_for_particular_entry = datetime_for_particular_entry + minute_delta
                df_current_date = df_current_date.append(particular_rows)
        
        if(df_current_date.shape[0] > 0):
            df_current_date = df_current_date.rename(columns = {'datetime': 'Date'})
            df_current_date.to_csv("TDAmeritrade-Minute-Data-By-Day-Split-Applied/" + str(current_date) + ".csv", index = False, columns = ['Date', 'Time', 'Ticker', 'open','high','low','close','volume'])
            print("File Created For: ", str(current_date), " : With Rows Count: ", str(df_current_date.shape[0]))

    else:
        print("INFO: Current Day is on Weekend")
    
