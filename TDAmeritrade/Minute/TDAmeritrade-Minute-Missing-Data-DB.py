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
sys.path.insert(2, '../../Email/')
import shutil # For File System Operations.
#from SP500 import FetchStocksList
import EmailService
from sqlalchemy import create_engine

database_connection_string = 'postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg'
engine = create_engine(database_connection_string)

db_symbol_ids = {}
with engine.connect() as con:    
    rows = con.execute(f'SELECT "Id", "Symbol" FROM public."Ticker"')
    for row in rows:
        #row[1] is the symbol and row[0] is the id
        db_symbol_ids[row[1]] = row[0]

today_date = datetime.datetime.today()
today_date_format = today_date.strftime(r"%Y-%m-%d")

#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
Tickerdata = pd.read_csv("../../Tickers/TickerList.csv")
ticker_list = Tickerdata["Symbol"].values.tolist()
total_ticker_list = len(ticker_list)
#ticker_list = ["AAPL", "GOOG"]
print("Total Tickers: ", len(ticker_list))

#Remove Redundant Tickers from ticker's list - All tickers that are not in TDAmeritrade but are there in our Ticker.csv file needs to be removed.
RedundantTickerdata = pd.read_csv("../../Tickers/TDAmeritradeRedundantTickerList.csv")
redundant_ticker_list = RedundantTickerdata["Symbol"].values.tolist()
for each_redundant_ticker in redundant_ticker_list:
    if(each_redundant_ticker in ticker_list):
        ticker_list.remove(each_redundant_ticker)

# variable for request of opening the URL
datafile = urllib.request.URLopener()

#Date and time (GMT): Wednesday, January 1, 1800 5:00:00 PM = startDate= -5364601200000
#Date and time (GMT): Friday, January 1, 2100 5:00:00 PM = endDate= 4102506000000

# The size of each step in days
day_delta = datetime.timedelta(days=1)

# datetime(year, month, day, hour, minute, second, microsecond) - Note these dates shall not start from weekend otherwise the TDAmeritrade API gives error. 
# Note these dates (start_date and end_date) both are inclusive.
start_date = [2023,4,3]
end_date = [2023,4,7]

start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2], tzinfo= pytz.UTC)
end_datetime = datetime.datetime(end_date[0],end_date[1], end_date[2], tzinfo= pytz.UTC)
print("DateTime: ", start_datetime)

start_date_timestamp = str(int(round(start_datetime.timestamp() * 1000)))
end_date_timestamp = str(int(round(end_datetime.timestamp() * 1000)))


print("Start: ", start_date_timestamp)
print("End: ", end_date_timestamp)

#Tickers List that are not found in TD Ameritrade
tickers_not_found = []
tickers_with_error = []
successful_tickers = []
current_snp_tickers_not_found = []
current_snp_tickers_with_error = []
old_snp_tickers_not_found = []
old_snp_tickers_with_error = []

db_df = pd.DataFrame(columns=['TickerId', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume'])

# ------------------- Code to Pull Out CSV files for all tickers ---------------------
count = 0
for ticker_symbol in ticker_list:
    count += 1
    db_df_ticker = pd.DataFrame(columns=['TickerId', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    print("DOWNLOADING TICKER COUNT: ", count, " : SYMBOL ", ticker_symbol)

    tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=day&period=1&frequencyType=minute&frequency=1&endDate=' + end_date_timestamp + '&startDate=' + start_date_timestamp + '&needExtendedHoursData=true'

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
            df_tickerextract["datetime"] = pd.to_datetime(df_tickerextract["datetime"])
            df_tickerextract['datetime'] = df_tickerextract['datetime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
            
            #Segregating Date and Time from datetime combined column.
            df_tickerextract["Date"] = pd.to_datetime(df_tickerextract["datetime"]).dt.strftime('%Y%m%d')
            df_tickerextract["Time"] = pd.to_datetime(df_tickerextract["datetime"]).dt.strftime('%H:%M:%S')

            df_tickerextract = df_tickerextract.drop(columns=['datetime'], axis=1)
            df_tickerextract.to_csv("Data-By-Ticker-Temporary/" + ticker_symbol + ".csv", index = False)
            
            #Adding data to df for database
            db_df_ticker["Date"] = df_tickerextract["Date"].astype(str)
            db_df_ticker["Time"] = df_tickerextract["Time"].astype(str)
            db_df_ticker["Open"] = df_tickerextract["open"]
            db_df_ticker["High"] = df_tickerextract["high"]
            db_df_ticker["Low"] = df_tickerextract["low"]
            db_df_ticker["Close"] = df_tickerextract["close"]
            db_df_ticker["Volume"] = df_tickerextract["volume"]
            db_df_ticker["TickerId"] = db_symbol_ids[ticker_symbol]
            db_df = pd.concat([db_df,db_df_ticker], ignore_index=True)

            successful_tickers.append(ticker_symbol)
        else:
            tickers_not_found.append(ticker_symbol)
            print("Data Not Found For: ", ticker_symbol)
        
    except:
        tickers_with_error.append(ticker_symbol)
        print("Error while processing for this ticker.")
    
print("Data Not Found For:\n", tickers_not_found)

# ----------------- Code to convert data files by ticker to data files by day -----------------
entries = os.listdir('Data-By-Ticker-Temporary/')
#Sort all files by name
entries.sort()


# ----------------- Adding Data in Database -----------------

with engine.connect() as con:    
    no_of_rows_inserted = db_df.to_sql('TDAmeritradeMinutePrice', engine, if_exists = "append", index = False)

# ----------------- Code to convert data files by ticker to data files by Individual Tickers -----------------

files_in_data_by_ticker_folder = os.listdir('TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/')

for entry in entries:
    df = pd.read_csv('Data-By-Ticker-Temporary/' + entry)
    if(entry in files_in_data_by_ticker_folder):
        df.to_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" + entry, mode = 'a', index = False, header = False, columns = ['Date', 'Time', 'open','high','low','close','volume'])
    else:
        df.to_csv("TDAmeritrade-Minute-Data-By-Ticker-Split-Applied/" + entry, index = False, columns = ['Date', 'Time', 'open','high','low','close','volume'])
    
    print("File Created For: ", entry, " : With Rows Count: ", str(df.shape[0]))


#Delete all files in Temporary Folder Data-By-Ticker-Temporary by deleting the entire directory
shutil.rmtree("Data-By-Ticker-Temporary")
#Create the folder again for future storage of temporary data.
os.mkdir("Data-By-Ticker-Temporary")


