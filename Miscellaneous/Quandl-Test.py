import quandl
import datetime
 
quandl.ApiConfig.api_key = 'WW1i3KUcaUhpg8yqzYcW'
 
def quandl_stocks(symbol, start_date=(2022, 1, 1), end_date=None):
    """
    symbol is a string representing a stock symbol, e.g. 'AAPL'
 
    start_date and end_date are tuples of integers representing the year, month,
    and day
 
    end_date defaults to the current date when None
    """
 
    query_list = ['WIKI' + '/' + symbol + '.' + str(k) for k in range(1, 13)]
    
    print(query_list)

    start_date = datetime.date(*start_date)
 
    if end_date:
        end_date = datetime.date(*end_date)
    else:
        end_date = datetime.date.today()
 
    return quandl.get(query_list, 
            returns='pandas', 
            start_date=start_date,
            end_date=end_date,
            collapse='daily',
            order='asc'
            )
 
 
if __name__ == '__main__':
 
    #apple_data = quandl_stocks('AAPL')
    dataset_data = quandl.Dataset('WIKI/AAPL').data(params={ 'start_date':'2017-01-01', 'end_date':'2018-01-01', 'collapse':'minute', 'transformation':'rdiff' })
    print(dataset_data.column_names)
    for i in dataset_data:
        print(i[0], "      ", i[1], "      ", i[2], "      ",  i[3], "      ", i[4], "      ", i[5] , "      ", i[6])