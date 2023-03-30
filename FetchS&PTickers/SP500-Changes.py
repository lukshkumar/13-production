import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_sp500_symbols():
    """
    Get all S&P 500 component stocks
    """
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#Selected_changes_to_the_list_of_S&P_500_components')
    soup = BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable', 'id' : 'changes'})
    df = pd.DataFrame(columns = ["Date", "Ticker Added", "Ticker Removed"])
    
    for row in table.findAll('tr')[2:]:
        changes = []
        changes.append(row.findAll('td')[0].text)
        changes.append(row.findAll('td')[1].text)
        changes.append(row.findAll('td')[3].text)
        df.loc[len(df)] = changes
    return df

def FetchStockChangesList():
    
    df_changes = get_sp500_symbols()
    
    df_changes.to_csv("df_changes.csv", index = False)
    
FetchStockChangesList()