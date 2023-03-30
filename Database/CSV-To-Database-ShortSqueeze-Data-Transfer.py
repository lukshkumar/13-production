import os
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg')

data_path = "../ShortSqueeze/Data/"
entries = os.listdir(data_path)
#Sort all files by name
entries.sort()

with engine.connect() as con:    
    for entry in entries:
        try:
            df = pd.read_excel(data_path + entry)
            df = df.rename({'ShortSqueeze.com Short Interest Data': 'Short Interest Data', 
                            '(abs)': '(abs) 52-wk', 
                            '(abs).1': '(abs) 200 day', 
                            '(abs).2': '(abs) 50 day', 
                            'Short Squeeze Ranking™': 'Short Squeeze Ranking'}, 
                            axis=1)
            no_of_rows_inserted = df.to_sql('ShortSqueeze', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {entry}')
        except Exception as e:
            print(e)
            print("Error occured for:", entry)
            