import pandas as pd
import datetime 

today_date = datetime.datetime.today()

#Preparing List for Email Report
today_date_format = today_date.strftime(r"%Y%m%d")
current_snp_tickers_filepath  = "..\..\snp\\" + today_date_format + ".csv"
current_snp_tickers = pd.read_csv(current_snp_tickers_filepath)["Symbol"].values.tolist()
print(len(current_snp_tickers))