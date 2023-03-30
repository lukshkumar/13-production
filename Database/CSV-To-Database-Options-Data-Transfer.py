import os
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg')

data_path = "../WinSCP/Data/Extracted-Data/"
entries = os.listdir(data_path)
#Sort all files by name
entries.sort()

with engine.connect() as con:    
    for entry in entries:
        try:
            df = pd.read_csv(data_path + entry)
            df["date"] = df["date"].astype(str)
            df["expiration"] = df["expiration"].astype(str)
            #print(df.head())
            no_of_rows_inserted = df.to_sql('CboeOptions', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {entry}')
        except Exception as e:
            print(e)
            print("Error occured for:", entry)