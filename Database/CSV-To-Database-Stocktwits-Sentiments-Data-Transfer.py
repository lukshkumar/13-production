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

data_path = "../Stocktwits/data/sentiments/"
entries = os.listdir(data_path)
#Sort all files by name
entries.sort()

with engine.connect() as con:    
    for entry in entries:
        try:
            df = pd.read_csv(data_path + entry)
            df["Date_scraped"] = df["Date_scraped"].astype(str)
            no_of_rows_inserted = df.to_sql('StocktwitsSentiment', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {entry}')
        except Exception as e:
            print(e)
            print("Error occured for:", entry)
            