import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
import pandas as pd

def read_database_connection_string():
    text_file = open(r"D:/Project Hamburg/13-production/db-connection-string.txt", "r")
    CONNECTION_STRING = text_file.read()
    text_file.close()
    return CONNECTION_STRING

engine = create_engine(read_database_connection_string())

#conn = psycopg2.connect(host = DB_HOST, database = DB_NAME, user = DB_USER, password = DB_PASS)

df = pd.DataFrame({'TickerId': [1], 'Date': ["19280103"], 'Open': [10],'High': [15], 'Low': [5], 'Close': [12], 'Volume': [100]})
print(df.head())
print(df.to_sql('TDAmeritradeDailyPrice', engine, if_exists = "append", index = False))

# with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
#     # cur.execute('UPDATE public."Ticker" SET "Name" = \'\' WHERE "Id" = 1')
#     # conn.commit()

#     # cur.execute('SELECT "Id" FROM public."Ticker" where "Symbol" = \'MMM\'')
#     # row = cur.fetchone()
#     # ticker_id = row["Id"]

    

# conn.close()