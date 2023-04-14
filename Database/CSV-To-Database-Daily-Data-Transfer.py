import os
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd

def read_database_connection_string():
    text_file = open(r"D:/Project Hamburg/13-production/db-connection-string.txt", "r")
    CONNECTION_STRING = text_file.read()
    text_file.close()
    return CONNECTION_STRING

engine = create_engine(read_database_connection_string())

daily_data_path = "../TDAmeritrade/Daily/TDAmeritrade-Daily-Data-By-Ticker-Split-Applied/"
entries = os.listdir(daily_data_path)
#Sort all files by name
entries.sort()

with engine.connect() as con:    
    for entry in entries:
        try:
            df = pd.read_csv(daily_data_path + entry)
            symbol = entry.replace(".csv", "")
            rows = con.execute(f'SELECT "Id" FROM public."Ticker" where "Symbol" = \'{symbol}\'')
            for row in rows:
                ticker_id = row[0]
            #print(symbol, ":", ticker_id)
            df.insert(loc=0, column='TickerId', value=ticker_id)
            df = df.rename({'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, axis=1)
            df["Date"] = df["Date"].astype(str)
            #print(df.head())
            no_of_rows_inserted = df.to_sql('TDAmeritradeDailyPrice', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {symbol}')
        except Exception as e:
            print(e)
            print("Error occured for:", symbol)