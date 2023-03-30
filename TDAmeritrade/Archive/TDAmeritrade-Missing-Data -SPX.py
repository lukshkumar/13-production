import urllib.request
import json
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
import datetime  
import sys
import datetime
sys.path.insert(1, '../Tickers/')

#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
# Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
# ticker_list = Tickerdata["Symbol"].values.tolist()
# print("Total Tickers: ", len(ticker_list))

# variable for request of opening the URL
datafile = urllib.request.URLopener()

#Date and time (GMT): Wednesday, January 1, 1800 5:00:00 PM = startDate= -5364601200000
#Date and time (GMT): Friday, January 1, 2100 5:00:00 PM = endDate= 4102506000000

# The size of each step in days
day_delta = datetime.timedelta(days=1)

# datetime(year, month, day, hour, minute, second, microsecond)
start_date = [1920,1,1]
end_date = [2022,5,17]

# start_datetime = datetime.datetime(start_date[0], start_date[1], start_date[2], 12)
# end_datetime = datetime.datetime(end_date[0],end_date[1], end_date[2])

# start_date_timestamp = str(int(round(start_datetime.timestamp() * 1000)))
# end_date_timestamp = str(int(round(end_datetime.timestamp() * 1000)))

# start_day = datetime.date(start_date[0], start_date[1], start_date[2])
# end_day = datetime.date(end_date[0],end_date[1], end_date[2])

# print("Start: ", start_date_timestamp)
# print("End: ", end_date_timestamp)

#Tickers List that are not found in TD Ameritrade
tickers_not_found = []

ticker_list = ["AAPL"]

#Code to Pull Out CSV files for all tickers
count = 0
for ticker_symbol in ticker_list:
    count += 1
    print("DOWNLOADING TICKER COUNT: ", count, " : SYMBOL ", ticker_symbol)

    #tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=month&period=1&frequencyType=daily&frequency=1&endDate=' + end_date_timestamp + '&startDate=' + start_date_timestamp + '&needExtendedHoursData=false'
    tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=month&period=1&frequencyType=daily&frequency=1&endDate=4102506000000&startDate=-5364601200000&needExtendedHoursData=false'
    #tickerextract = 'https://api.tdameritrade.com/v1/marketdata/' + ticker_symbol + '/pricehistory?apikey=CATOKINCAID&periodType=day&period=1&frequencyType=minute&frequency=1&endDate=' + '4102506000000' + '&startDate=' + '5364601200000' + '&needExtendedHoursData=false'

    datafound = False
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
            # x = str(x)
            # x = int(x[0:len(x) -3])
            # x = datetime.datetime.fromtimestamp(x)  
            
            x= datetime.datetime(1970, 1, 1) +  datetime.timedelta(seconds=x/1000)
            return x

        df_tickerextract['datetime'] = df_tickerextract['datetime'].apply(lambda x: TrimTrailing(x))

        df_tickerextract["datetime"] = pd.to_datetime(df_tickerextract["datetime"]).dt.strftime('%Y%m%d')

        df_tickerextract.to_csv("Data-By-Ticker/" + ticker_symbol + ".csv", index = False)
    else:
        tickers_not_found.append(ticker_symbol)
        print("Data Not Found For: ", ticker_symbol)
    
print("Data Not Found For:\n", tickers_not_found )

