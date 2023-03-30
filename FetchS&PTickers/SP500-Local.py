import requests
import pandas as pd
from bs4 import BeautifulSoup
import sys
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')
import pandas as pd # For Data processing and Operations on data returned by tabula.
import json
import datetime
import requests as r

#Logging the entry when the script has run
# Open a file with access mode 'a'
file_object = open('SP500-And-Splits-logfile.txt', 'a')
file_object.write('Last Run: ' + str(datetime.datetime.now()) + "\n")
# Close the file
file_object.close()

def get_sp500_symbols():
    """
    Get all S&P 500 component stocks
    """
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    stocks = []
    for row in table.findAll('tr')[1:]:
        symbol = row.findAll('td')[0].text
        stocks.append(symbol.replace("\n",""))
    return stocks

def FetchStocksList():
    
    live_list = get_sp500_symbols()
    
    print("Total Number of Stocks in Web Scraped List: ", len(live_list))
    #Fetching the Ticker List
    Tickerdata = pd.read_csv("..\Tickers\TickerList.csv")
    ticker_list = Tickerdata["Symbol"].values.tolist()
    csv_file_list = list(ticker_list)
    
    print("Total Number of Stocks in CSV List: ", len(csv_file_list))
    
    count = 0
    list_of_missing_tickers = []
    for each_ticker in live_list:
        if each_ticker not in csv_file_list:
            print("TICKER NOT FOUND: ", each_ticker)
            list_of_missing_tickers.append(each_ticker)
            count += 1
    
    print("Total Number of Tickers Not Found In CSV: ", count)
    missing_data_df = pd.DataFrame(list_of_missing_tickers, columns = ['Symbol'])
    
    Tickerdata = Tickerdata.append(missing_data_df)
    Tickerdata.to_csv("..\Tickers\TickerList.csv", index = False)
    print("Missing Data Added into Ticker List CSV File Successfully!")


def FetchSplitsList():
    
    #Fetching the Ticker List
    Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
    ticker_list_splited = Tickerdata["Symbol"].values.tolist()
    print("Total Tickers: ", len(ticker_list_splited))

    #ticker_list_splited = ["WRB"]

    print("------------------ Extracting Ticker Split Data from Financial Modeling ------------------")
    day_delta = datetime.timedelta(days=30)
    
    #Automatic Date 
    from_date = str(datetime.datetime.now().date() - day_delta)
    #print("From Date: ", from_date)
    # Custom Date
    #from_date = "2000-01-01"
    
    json_data = r.get("https://financialmodelingprep.com/api/v3/stock_split_calendar?from=" + from_date + "&apikey=aa8b4631b2ad1b2704741d1a2d6a2611")
    data =  json.loads(json_data.content)
    df = pd.json_normalize(data)
    df =  df.rename(columns = {"date" : "Date"})
    df["Stock Splits"] = 0.0
    df["Ticker"] = df["symbol"]
    
    for i in range(df.shape[0]):
        df.loc[i, "Stock Splits"] = df.loc[i, "numerator"] / df.loc[i, "denominator"]
    
    df = df.drop(columns=["label", "symbol", "numerator", "denominator"])

    trimmed_df = pd.DataFrame(columns=["Date", "Stock Splits", "Ticker"])

    for i in range(df.shape[0]):
        if (df.loc[i, "Ticker"] in ticker_list_splited):
            trimmed_df.loc[len(trimmed_df)] = df.iloc[i,:]
            
    trimmed_df.to_csv("../Yahoo/Splits.csv", index = False)


FetchStocksList()

FetchSplitsList()