import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../../Tickers/')
sys.path.insert(2,'../../')

from SP500 import FetchStocksList
import tosdb
import time
import pandas as pd
import datetime
from pytz import timezone
import json

#Fetching the Live Stocks List and Updating TickerList.csv
#FetchStocksList()

#Fetching the Ticker List
#Tickerdata = pd.read_csv("../Tickers/TickerList.csv")
#items_to_fetch = Tickerdata["Symbol"].values.tolist()

#If Testing on Hard Coded Items then uncomment below line.
items_to_fetch = ["SPX"]

print("Total Tickers: ", len(items_to_fetch))

#Reading the Configurations from JSON file

f = open('../../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

# Topics
topics_to_fetch = configurations["TOS"]["topics_to_fetch"]
name_of_the_event = configurations["TOS"]["name_of_the_event"]
time_start = configurations["TOS"]["time_start"]
time_end = configurations["TOS"]["time_end"]
tz = timezone(configurations["TOS"]["time_zone"])

#items_to_fetch = ["QQQ", "SPY", "AMZN", "GOOG", "TSLA","MCD", "SPX","VIX"]
#topics_to_fetch = ['LAST']
#items_to_fetch = ["GBP/USD", "EUR/JPY", "EUR/USD", "AUD/USD"]
# FORMAT: [YYYY, MM, DD, HH, MM, SS]


print("--------------------- BUILDING CONNECTION --------------------")

print("INFO: Connecting to TOS Data and Server")
print(tosdb.init(dllpath="C:/TOSDataBridge-master/bin/Release/x64/tos-databridge-0.9-x64.dll"))

print("INFO: Checking Connection State")
print(tosdb.connected())

print("INFO: Checking ENGINE and TOS both Connected Successfully!")
print(tosdb.connection_state() == tosdb.CONN_ENGINE_TOS)

print(dir(tosdb.TOPICS))

print("--------------------- FETCHING DATA --------------------")

#time.sleep(3)
block1 = tosdb.TOSDB_DataBlock(1000, True)

# List of Topics
#print("INFO: List of Topics")
#print(dir(tosdb.TOPICS))

# Adding Topics
for each_topic in topics_to_fetch:
    block1.add_topics(each_topic)
for each_item in items_to_fetch:
    block1.add_items(each_item)

list_of_items = list(block1.items())
list_of_topics  = list(block1.topics())

list_of_topics.insert(0, "Date")
list_of_topics.insert(1, "Timestamp")

#FORMAT: YYYY, MM, DD, HH, MM, SS

tz_time_start =  datetime.datetime(time_start[0], time_start[1], time_start[2], time_start[3], time_start[4], time_start[5])
tz_time_end  = datetime.datetime(time_end[0], time_end[1], time_end[2], time_end[3], time_end[4], time_end[5])

total_df = pd.DataFrame()
time_matched = False

print("-----------------The Script Will Fetch Data Once the Given Date and Time Reach-------------------")
while(True):
    
    #If Testing for fewer Hard Coded items and do not more than one entry per second then uncomment below line.
    time.sleep(15)
    
    tz_time_now = datetime.datetime.now(tz).replace(tzinfo=None)
    #print("Current Time: ", str(tz_time_now).split('.')[0])
    if((tz_time_now >= tz_time_start) and (tz_time_now <= tz_time_end)):
        
        print("Current Time: ", str(tz_time_now).split('.')[0])
      
        time_matched = True
        #Total Frame
        current_data1 = block1.total_frame(date_time = False, labels = True)

        df = pd.DataFrame(columns = list_of_topics)

        for key, value in current_data1.items():
            temp_list = []
            temp_list.append(datetime.datetime.now(tz).replace(tzinfo=None).strftime("%Y-%m-%d"))
            temp_list.append(datetime.datetime.now(tz).replace(tzinfo=None).strftime("%H:%M:%S"))
            for each_topic in range(len(list_of_topics) - 2):
                temp_list.append(value[each_topic])
            df_temp = pd.DataFrame(data = [temp_list], columns = list_of_topics, index = [key])    
            df = df.append(df_temp)    

        total_df = total_df.append(df) 
        print(df)
    
    elif(time_matched):
        time_matched = False
        total_df.to_csv("ThinkOrSwim-Data/" + name_of_the_event + ".csv")
        break


# Historical Data
#print("INFO: Historical Data")
#snapshot_all = block1.stream_snapshot("SPY","HIGH", date_time = True, end = 1000, beg = 0)
#print("LENGTH: ", len(snapshot_all))
#print(snapshot_all)

#snapshot_all = block1.get("SPY","LOW", date_time = False, indx = 0)
#print(snapshot_all)
