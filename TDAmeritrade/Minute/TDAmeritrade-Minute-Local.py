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

#Condition to see if the scipt has already been ran today. Ideally the script should only run once a day otherwise we can have duplicate data.
with open('TDAmeritrade-Minute-logfile.txt', 'rb') as f:
    try:  # catch OSError in case of a one line file 
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
    except OSError:
        f.seek(0)
    last_line = f.readline().decode()

today_date = datetime.datetime.today() - datetime.timedelta(days=0)
today_date_format = today_date.strftime(r"%Y-%m-%d")

if(last_line.split()[2] == today_date_format):

    #Logging the entry when the script has run
    # Open a file with access mode 'a'
    file_object = open('TDAmeritrade-Minute-logfile.txt', 'a')

    file_object.write('Last Run: ' + str(datetime.datetime.now()) + "\n")

    # Close the file
    file_object.close()

    #Fetching the Live Stocks List and Updating TickerList.csv
    #FetchStocksList()

    #Fetching the Ticker List
    Tickerdata = pd.read_csv("../../Tickers/TickerList.csv")
    ticker_list = Tickerdata["Symbol"].values.tolist()
    total_ticker_list = len(ticker_list)
    ticker_list = ['AAPL']
    print("Total Tickers: ", len(ticker_list))

    # variable for request of opening the URL
    datafile = urllib.request.URLopener()

    #Date and time (GMT): Wednesday, January 1, 1800 5:00:00 PM = startDate= -5364601200000
    #Date and time (GMT): Friday, January 1, 2100 5:00:00 PM = endDate= 4102506000000

    # The size of each step in days
    day_delta = datetime.timedelta(days=1)

    # datetime(year, month, day, hour, minute, second, microsecond) - Note these dates shall not start from weekend otherwise the TDAmeritrade API gives error. 
    # Note these dates (start_date and end_date) both are inclusive.
    start_date = [today_date.year,today_date.month,today_date.day]
    end_date = start_date

    start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2], 9,30,0, tzinfo= pytz.UTC)
    end_datetime = datetime.datetime(end_date[0],end_date[1], end_date[2], 16,0,0, tzinfo= pytz.UTC)
    print("DateTime: ", start_datetime)

    start_date_timestamp = str(int(round(start_datetime.timestamp() * 1000)))
    end_date_timestamp = str(int(round(end_datetime.timestamp() * 1000)))

    start_day = datetime.date(start_date[0], start_date[1], start_date[2])

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

    # ------------------- Code to Pull Out CSV files for all tickers ---------------------

    count = 0
    for ticker_symbol in ticker_list:
        count += 1
        print("DOWNLOADING TICKER COUNT: ", count, " : SYMBOL ", ticker_symbol)

        tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=HO6QTTGXMJQSPGKJRPVVYTGVXN4YTZ6X&periodType=day&period=1&frequencyType=minute&frequency=1&endDate=' + end_date_timestamp + '&startDate=' + start_date_timestamp + '&needExtendedHoursData=false'
        print(tickerextract)
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

    """"
    df_current_date = pd.DataFrame()
    current_date = str(start_day).replace("-","")
    print(current_date)
        
    for entry in entries:
        df = pd.read_csv('Data-By-Ticker-Temporary/'+ entry)
        df.insert(loc=0, column='Ticker', value=entry.replace(".csv", ""))
        
        if(df.shape[0] > 0):
            df_current_date = df_current_date.append(df)

    if(df_current_date.shape[0] > 0):
        df_current_date.to_csv("TDAmeritrade-Minute-Data-By-Day-Split-Applied/" + current_date + ".csv", index = False, columns = ['Date', 'Time', 'Ticker', 'open','high','low','close','volume'])
        print("File Created For: ", current_date, " : With Rows Count: ", str(df_current_date.shape[0]))

    """
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
    """
    #Preparing List for Email Report
    today_date_format = today_date.strftime(r"%Y%m%d")
    current_snp_tickers_filepath  = "..\..\snp\\" + today_date_format + ".csv"
    current_snp_tickers = pd.read_csv(current_snp_tickers_filepath)["Symbol"].values.tolist()

    for ticker in tickers_not_found:
        if(ticker in current_snp_tickers):
            current_snp_tickers_not_found.append(ticker)
        else:
            old_snp_tickers_not_found.append(ticker)
    for ticker in tickers_with_error:
        if(ticker in current_snp_tickers):
            current_snp_tickers_with_error.append(ticker)
        else:
            old_snp_tickers_with_error.append(ticker)


    Subject = "Report: TDAmeritrade Minute"
    Body = "<h1><u><center>TDAmeritrade Report - Minute Data<center></u></h1>"

    tickers_count_ratio  = " (" + str(len(successful_tickers)) + "/" +  str(total_ticker_list) + ")"
    Body += "<h2 style='color:green'><u>Successful Tickers</u>" + str(tickers_count_ratio) + "</h2>"
    Body += ", ".join(successful_tickers)

    tickers_count_ratio  = " (" + str(len(current_snp_tickers_not_found) + len(current_snp_tickers_with_error) + len(old_snp_tickers_not_found) + len(old_snp_tickers_with_error)) + "/" +  str(total_ticker_list) + ")"
    Body += "<h2 style='color:red'><u>Failure Tickers</u>" + str(tickers_count_ratio) + "</h2>"

    tickers_count_ratio  = " (" + str(len(current_snp_tickers_not_found) + len(current_snp_tickers_with_error)) + "/" +  str(total_ticker_list) + ")"
    Body += "<h3><u><i>Current SNP</i></u>" + str(tickers_count_ratio) + "</h3>"
    Body += ", ".join(current_snp_tickers_not_found)
    if(len(current_snp_tickers_with_error) > 0):
        Body += "<h3 style='color:red'><u><i>Failure Tickers - Error While Fetching</i></u></h3>"
        Body += ", ".join(current_snp_tickers_with_error)


    tickers_count_ratio  = " (" + str(len(old_snp_tickers_not_found) + len(old_snp_tickers_with_error)) + "/" +  str(total_ticker_list) + ")"
    Body += "<h3><u><i>Old SNP</i></u>" + str(tickers_count_ratio) + "</h3>"
    Body += ", ".join(old_snp_tickers_not_found)
    if(len(old_snp_tickers_with_error) > 0):
        Body += "<h3 style='color:red'><u><i>Failure Tickers - Error While Fetching</i></u></h3>"
        Body += ", ".join(old_snp_tickers_with_error)


    Body += "<i><br><br>Thank you for reading the report.</i>"

    EmailService.SendEmail(Subject, Body)

else:
    print("Error: Script has already ran for today.")

    """