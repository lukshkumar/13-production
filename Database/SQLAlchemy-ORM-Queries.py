import requests
from bs4 import BeautifulSoup
import json
from sqlalchemy import create_engine
from sqlalchemy import create_engine, MetaData, update

database_connection_string = 'postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg'
engine = create_engine(database_connection_string)
meta = MetaData(bind=engine)
MetaData.reflect(meta)
TDAmeritradeDailyPrice = meta.tables['TDAmeritradeDailyPrice']

# update
u = update(TDAmeritradeDailyPrice)
u = u.values({"Open": TDAmeritradeDailyPrice.c.Open + 1, "High": 10, "Low": 10, "Close": 10})
u = u.where(TDAmeritradeDailyPrice.c.TickerId == 45).where(TDAmeritradeDailyPrice.c.Date >= "2023-01-18")
engine.execute(u)

"""
symbol_ids = {}
with engine.connect() as con:    
    try:
        rows = con.execute(f'SELECT "Id", "Symbol" FROM public."Ticker"')
        print(type(rows))
        for row in rows:
            #row[1] is the symbol and row[0] is the id
            symbol_ids[row[1]] = row[0]
        print(symbol_ids)
        #print(symbol, ":", ticker_id)
        # df.insert(loc=0, column='TickerId', value=ticker_id)
        # df = df.rename({'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, axis=1)
        # df["Date"] = df["Date"].astype(str)
        # #print(df.head())
        # no_of_rows_inserted = df.to_sql('TDAmeritradeDailyPrice', engine, if_exists = "append", index = False)
        # print(f'{no_of_rows_inserted} rows inserted for {symbol}')
    except Exception as e:
        print("Error occured:", e)
"""