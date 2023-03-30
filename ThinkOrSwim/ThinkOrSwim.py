import sys
# insert at 1, 0 is the script path (or '' in REPL)
sys.path.insert(1, '../Tickers/')
sys.path.insert(2,'../')

import tosdb
import time
import pandas as pd
import datetime
from pytz import timezone
import json
import os

#Reading the Configurations from JSON file

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()

# Topics
topics_to_fetch = configurations["TOS-Daily"]["topics_to_fetch"]
#Items
items_to_fetch = configurations["TOS-Daily"]["items_to_fetch"]
#Filenames
items_csv_filenames = configurations["TOS-Daily"]["items_csv_filenames"]

tz = timezone(configurations["TOS-Daily"]["time_zone"])

print("--------------------- BUILDING CONNECTION --------------------")

print("INFO: Connecting to TOS Data and Server")
print(tosdb.init(dllpath="C:/TOSDataBridge-master/bin/Release/x64/tos-databridge-0.9-x64.dll"))

print("INFO: Checking Connection State")
print(tosdb.connected())

print("INFO: Checking ENGINE and TOS both Connected Successfully!")
print(tosdb.connection_state() == tosdb.CONN_ENGINE_TOS)


print("--------------------- FETCHING DATA --------------------")

#time.sleep(3)
block1 = tosdb.TOSDB_DataBlock(1000, True)

# Adding Topics
for each_topic in topics_to_fetch:
    block1.add_topics(each_topic)
for each_item in items_to_fetch:
    block1.add_items(each_item)

list_of_items = list(block1.items())
list_of_topics  = list(block1.topics())

list_of_topics.insert(0, "date")
#list_of_topics.insert(1, "time")
list_of_topics.insert(1, "symbol")

#FORMAT: YYYY, MM, DD, HH, MM, SS

tz_time_start  = datetime.time(8, 0, 0)
tz_time_end  = datetime.time(20, 0, 0)

time_matched = False

tz_time_now = datetime.datetime.now(tz).replace(tzinfo=None)
tz_time_now = datetime.datetime.strptime(tz_time_now.strftime('%H:%M:%S'), '%H:%M:%S').time()

tz_date_now = datetime.datetime.now(tz).date()

print("Time Start: ", tz_time_start)
print("Time End: ", tz_time_end)
print("Time Now: ", tz_time_now)

if(tz_date_now.weekday() < 5 and tz_time_now >= tz_time_start and tz_time_now <= tz_time_end):
    
    print("INFO: Scripting is fetching the data...")
    
    #If Testing for fewer Hard Coded items and do not more than one entry per second then comment below line.
    time.sleep(10)
    
    #Total Frame
    current_data1 = block1.total_frame(date_time = False, labels = True)

    df = pd.DataFrame(columns = list_of_topics)

    for key, value in current_data1.items():
        temp_list = []
        temp_list.append(datetime.datetime.now(tz).replace(tzinfo=None).strftime("%Y-%m-%d"))
        temp_list.append(key)
        for each_topic in range(len(list_of_topics) - 2):
            temp_list.append(value[each_topic])
        df_temp = pd.DataFrame(data = [temp_list], columns = list_of_topics, index = [key])    
        df = df.append(df_temp)    

    print(df)

    for item in range(len(items_to_fetch)):

        df_daily_symbol_filename = "ThinkOrSwim-Data/Daily/" + items_csv_filenames[item] + ".csv"
        
        df_daily_symbol = df[df['symbol'] == items_to_fetch[item]]
        df_daily_symbol['filename'] = items_csv_filenames[item]
        
        if os.path.isfile(df_daily_symbol_filename): 
            df_daily_complete = pd.read_csv(df_daily_symbol_filename)
            if(df_daily_complete.loc[len(df_daily_complete) - 1, "date"] == df_daily_symbol.iloc[0,0]):
                df_daily_complete.loc[len(df_daily_complete) - 1] = df_daily_symbol.iloc[0]
            else:
                df_daily_complete = df_daily_complete.append(df_daily_symbol)
            df_daily_complete.to_csv(df_daily_symbol_filename, index = False)
        else:
            df_daily_symbol.to_csv(df_daily_symbol_filename, index = False)
    
