import json
import pandas as pd
import datetime  
import os
import zipfile
import sys
import datetime
from pytz import timezone
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')

# The size of each step in days
day_delta = datetime.timedelta(days=1)
f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

tz = timezone(configurations["WinSCP"]["time_zone"])

print("The WinSCP Program is Runing.......")

#Fetching the file from WinSCP SFTP Server.
#os.system('winscp.com /command "option batch abort" "option confirm off" "open sftp://mordecai_projecthamburg_com:Stockchartx!00@sftp.datashop.livevol.com -hostkey=""ssh-rsa 2048 9f:58:fd:a0:04:55:54:0a:f4:b9:78:75:f3:f3:6a:92""" "synchronize local -filemask=""*>=1D"" C:\\Users\Administrator\Desktop\StockChartX-PROD\WinSCP\Data\Zip-Data-New /subscriptions/order_000024948/item_000029903" "pause"')

# datetime(year, month, day, hour, minute, second, microsecond)
new_zip_file_path = "Data/Zip-Data-New/"
zip_file_path = "Data/Zip-Data/"
extracted_file_path = "Data/Extracted-Data/"
dir_list = os.listdir(new_zip_file_path)
#dir_list = ["UnderlyingOptionsEODCalcs_2021-07-01.zip"]

for file in dir_list:
    if(file.endswith(".zip")):

        zip_filename = file
        csv_filename_original = zip_filename.replace(".zip", ".csv")
        csv_filename = zip_filename.replace("UnderlyingOptionsEODCalcs_", "").replace("-", "").replace(".zip", ".csv")
        
        print(csv_filename)
        
        #Only move file when it hasn't already been moved.
        if not os.path.isfile(zip_file_path + zip_filename):
            #Move this Newly Fetched Zip file into Zip file folder containing all the files.
            os.rename(new_zip_file_path + zip_filename, zip_file_path + zip_filename)

        #Extract Zip File
        z = zipfile.ZipFile(zip_file_path + zip_filename)
        z.extractall(extracted_file_path)

        #Only rename file when it hasn't already been renamed.
        if not os.path.isfile(extracted_file_path + csv_filename):
            #Renaming the extracted csv file so to only have the date in csv filename rather than everything.
            os.rename(extracted_file_path + csv_filename_original, extracted_file_path + csv_filename)

        df = pd.read_csv(extracted_file_path + csv_filename)
        df = df.rename(columns={"underlying_symbol" : "Ticker", 
        "quote_date" : "date", 
        "trade_volume" : "TotalVolume",
        "bid_size_1545" : "bid_size",
        "bid_1545" : "bid",
        "ask_size_1545" : "ask_size",
        "ask_1545" : "ask",
        "underlying_bid_1545" : "underlying_bid",
        "underlying_ask_1545" : "underlying_ask",
        "implied_underlying_price_1545" : "implied_underlying_price",
        "active_underlying_price_1545" : "active_underlying_price",
        "implied_volatility_1545" : "implied_volatility",
        "delta_1545" : "delta",
        "gamma_1545" : "gamma",
        "theta_1545" : "theta",
        "vega_1545" : "vega",
        "rho_1545" : "rho",
        "open" : "OpenPrice",
        "high" : "HighPrice",
        "low" : "LowPrice",
        "close" : "ClosePrice",
        })

        df["date"] = pd.to_datetime(df["date"]).dt.strftime('%Y%m%d')
        df["expiration"] = pd.to_datetime(df["expiration"]).dt.strftime('%Y%m%d')

        for i in range(df.shape[0]):

            df.loc[i, 'Ticker'] = "." + str(df.loc[i, 'root']) + str(df.loc[i, 'expiration']) + str(df.loc[i, 'option_type']) + str(int(df.loc[i, 'strike']))

        df.to_csv(extracted_file_path + csv_filename, index = False)
        
print("INFO: Program completed Successfully.")


