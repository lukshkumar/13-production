"""
1. Go to https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
2. list all S&P 500 component stocks
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_sp500_symbols():
    """
    Get all S&P 500 component stocks
    """
    print("-------------------- Scraping S&P500 Stocks and Updating TickerList.csv --------------------")
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
