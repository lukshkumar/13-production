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
#from SP500 import FetchStocksList

#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
#Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
#ticker_list = Tickerdata["Symbol"].values.tolist()
ticker_list = ["SPX"]
print("Total Tickers: ", len(ticker_list))

# variable for request of opening the URL
datafile = urllib.request.URLopener()

#Date and time (GMT): Wednesday, January 1, 1800 5:00:00 PM = startDate= -5364601200000
#Date and time (GMT): Friday, January 1, 2100 5:00:00 PM = endDate= 4102506000000

# The size of each step in days
day_delta = datetime.timedelta(days=1)

# datetime(year, month, day, hour, minute, second, microsecond)
start_date = [1920,1,1]
end_date = [2022,3,22]

start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2], 12)
end_datetime = datetime.datetime(end_date[0],end_date[1], end_date[2])

start_date_timestamp = str(int(round(start_datetime.timestamp() * 1000)))
end_date_timestamp = str(int(round(end_datetime.timestamp() * 1000)))

start_day = datetime.date(start_date[0], start_date[1], start_date[2])
end_day = datetime.date(end_date[0],end_date[1], end_date[2])

print("Start: ", start_date_timestamp)
print("End: ", end_date_timestamp)

#Tickers List that are not found in TD Ameritrade
tickers_not_found = []

#ticker_list = ["AAPL", "GOOG"]

#Code to Pull Out CSV files for all tickers
count = 0
for ticker_symbol in ticker_list:
    count += 1
    print("DOWNLOADING TICKER COUNT: ", count, " : SYMBOL ", ticker_symbol)

    tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=month&period=1&frequencyType=daily&frequency=1&endDate=' + end_date_timestamp + '&startDate=' + start_date_timestamp + '&needExtendedHoursData=false'

    datafound = False
    while(not datafound):
        try:
            #retreiving the XML data and storing in file in JSON format
            datafile.retrieve(tickerextract, "JSON_Files/" + ticker_symbol + ".json")
            datafound = True
        except:
            print("No Data Found Yet.")
    #opening the JSON file and retrieving its data
    data = json.load(open("JSON_Files/" + ticker_symbol + '.json'))
    
    #converting JSON 
    df_tickerextract = DataFrame.from_dict(json_normalize(data['candles']), orient='columns')
    
    if(df_tickerextract.shape[0] > 0):
    
        def TrimTrailing(x):
            x = str(x)
            x = int(x[0:len(x) -3])
            x = datetime.datetime.fromtimestamp(x)  
            return x

        df_tickerextract['datetime'] = df_tickerextract['datetime'].apply(lambda x: TrimTrailing(x))

        df_tickerextract["datetime"] = pd.to_datetime(df_tickerextract["datetime"]).dt.strftime('%Y%m%d')

        df_tickerextract.to_csv("Data-By-Ticker/" + ticker_symbol + ".csv", index = False)
    else:
        tickers_not_found.append(ticker_symbol)
        print("Data Not Found For: ", ticker_symbol)
    
print("Data Not Found For:\n", tickers_not_found )

entries = os.listdir('Data-By-Ticker/')

#Sort all files by name
entries.sort()

#Remove Extra files
#entries.remove(".ipynb_checkpoints")


for i in range((end_day - start_day).days):
    df_current_date = pd.DataFrame()
    current_date = int(str(start_day + i*day_delta).replace("-",""))
    
    for entry in entries:
        df = pd.read_csv("Data-By-Ticker/" + entry)
        particular_row = df.loc[df['datetime'] == current_date, :]
        particular_row.insert(loc=0, column='Ticker', value=entry.replace(".csv", ""))
        if(particular_row.shape[0] > 0):
            df_current_date = df_current_date.append(particular_row)
    
    if(df_current_date.shape[0] > 0):
        df_current_date = df_current_date.drop(['datetime'], axis =1)
        df_current_date.to_csv("TDAmeritrade-Data-By-Day-Split-Applied/" + str(current_date) + ".csv", index = False)
        print("File Created For: ", str(current_date), " : With Ticker Count: ", str(df_current_date.shape[0]))
    

