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
import pytz
sys.path.insert(1, '../Tickers/')
import shutil # For File System Operations.
#from SP500 import FetchStocksList

#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
Tickerdata = pd.read_csv("../../Tickers/TickerList.csv")
ticker_list = Tickerdata["Symbol"].values.tolist()
#ticker_list = ["$SPX.X", "SPY"]
print("Total Tickers: ", len(ticker_list))

# variable for request of opening the URL
datafile = urllib.request.URLopener()

#Date and time (GMT): Wednesday, January 1, 1800 5:00:00 PM = startDate= -5364601200000
#Date and time (GMT): Friday, January 1, 2100 5:00:00 PM = endDate= 4102506000000

# The size of each step in days
day_delta = datetime.timedelta(days=1)

# datetime(year, month, day, hour, minute, second, microsecond)
start_date = [1928,1,1]
end_date = [2023,1,28]

start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2], 9,30,0, tzinfo= pytz.UTC)
end_datetime = datetime.datetime(end_date[0],end_date[1], end_date[2], 16,0,0, tzinfo= pytz.UTC)
print("DateTime: ", start_datetime)

start_date_timestamp = str(int(round(start_datetime.timestamp() * 1000)))
end_date_timestamp = str(int(round(end_datetime.timestamp() * 1000)))

start_day = datetime.date(start_date[0], start_date[1], start_date[2])
end_day = datetime.date(end_date[0],end_date[1], end_date[2])

print("Start: ", start_date_timestamp)
print("End: ", end_date_timestamp)

#Tickers List that are not found in TD Ameritrade
tickers_not_found = []

#Code to Pull Out CSV files for all tickers
count = 0
for ticker_symbol in ticker_list:
    count += 1
    print("DOWNLOADING TICKER COUNT: ", count, " : SYMBOL ", ticker_symbol)

    tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=month&period=1&frequencyType=daily&frequency=1&endDate=' + end_date_timestamp + '&startDate=' + start_date_timestamp + '&needExtendedHoursData=true'

    try:
        #retreiving the XML data and storing in file in JSON format
        datafile.retrieve(tickerextract, "JSON_Files/" + ticker_symbol + ".json")
        #opening the JSON file and retrieving its data
        data = json.load(open("JSON_Files/" + ticker_symbol + '.json'))
        #converting JSON 
        df_tickerextract = DataFrame.from_dict(json_normalize(data['candles']), orient='columns')
        
        if(df_tickerextract.shape[0] > 0):
        
            def EpochTimestampToDateTime(x):
                x= datetime.datetime(1970, 1, 1) +  datetime.timedelta(seconds=x/1000) 
                return x

            df_tickerextract['datetime'] = df_tickerextract['datetime'].apply(lambda x: EpochTimestampToDateTime(x))
            df_tickerextract["Date"] = pd.to_datetime(df_tickerextract["datetime"]).dt.strftime('%Y%m%d')

            df_tickerextract = df_tickerextract.drop(columns=['datetime'], axis=1)
            df_tickerextract.to_csv("Data-By-Ticker-Temporary/" + ticker_symbol + ".csv", index = False)
        else:
            tickers_not_found.append(ticker_symbol)
            print("Data Not Found For: ", ticker_symbol)
            
    except:
        print("Error while processing for this ticker.")

print("Data Not Found For:\n", tickers_not_found )

# ----------------- Code to convert data files by ticker to data files by day -----------------

entries = os.listdir('Data-By-Ticker-Temporary/')
#Sort all files by name
entries.sort()

"""
for i in range(((end_day - start_day).days) + 1):

    df_current_date = pd.DataFrame()
    current_date_in_date_format = start_day + i*day_delta
    current_day_weekday = current_date_in_date_format.weekday()
    current_date = int(str(current_date_in_date_format).replace("-",""))
    print(current_date)

    #We don't have to fetch or convert data on weekends as we don't have any on weekends therefore keeping this condition.
    if(current_day_weekday < 5):

        for entry in entries:
            df = pd.read_csv("Data-By-Ticker-Temporary/" + entry)
            particular_row = df.loc[df['Date'] == current_date, :]
            particular_row.insert(loc=0, column='Ticker', value=entry.replace(".csv", ""))
            if(particular_row.shape[0] > 0):
                df_current_date = df_current_date.append(particular_row)
        
        if(df_current_date.shape[0] > 0):
            #We can drop the date column from the daily data but we are not dropping yet just to keep it for reference.
            #df_current_date = df_current_date.drop(['Date'], axis =1)
            df_current_date.to_csv("TDAmeritrade-Daily-Data-By-Day-Split-Applied/" + str(current_date) + ".csv", index = False, columns = ['Ticker', 'Date', 'open','high','low','close','volume'])
            print("File Created For: ", str(current_date), " : With Ticker Count: ", str(df_current_date.shape[0]))
        
    else:
        print("INFO: Current Day is on Weekend")

"""

# ----------------- Code to convert data files by ticker to data files by Individual Tickers -----------------
files_in_data_by_ticker_folder = os.listdir('TDAmeritrade-Daily-Data-By-Ticker-Split-Applied/')

for entry in entries:
    df = pd.read_csv('Data-By-Ticker-Temporary/' + entry)
    if(entry in files_in_data_by_ticker_folder):
        df.to_csv("TDAmeritrade-Daily-Data-By-Ticker-Split-Applied/" + entry, mode = 'a', index = False, header = False, columns = ['Date', 'open','high','low','close','volume'])
    else:
        df.to_csv("TDAmeritrade-Daily-Data-By-Ticker-Split-Applied/" + entry, index = False, columns = ['Date', 'open','high','low','close','volume'])
    
    print("File Created For: ", entry, " : With Rows Count: ", str(df.shape[0]))

#Delete all files in Temporary Folder Data-By-Ticker-Temporary by deleting the entire directory
shutil.rmtree("Data-By-Ticker-Temporary")
#Create the folder again for future storage of temporary data.
os.mkdir("Data-By-Ticker-Temporary")

    

