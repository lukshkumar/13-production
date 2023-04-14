import pandas as pd 
from pystocktwits import *
from datetime import datetime
import os
from pytz import timezone
twit = Streamer()
import numpy as np
from sqlalchemy import create_engine

#Connecting to the database.
def read_database_connection_string():
    text_file = open(r"D:/Project Hamburg/13-production/db-connection-string.txt", "r")
    CONNECTION_STRING = text_file.read()
    text_file.close()
    return CONNECTION_STRING

engine = create_engine(read_database_connection_string())

def create_dataset(list_of_stocks, tracker):

    """Createse dataframes from stocks
    Args : 
    list_of_stocks : A list of all the stocks(Strings)  need to get posts from 

      Args:
            symbol_id:	Ticker symbol, Stock ID, or
                        RIC code of the symbol (Required)
            since:	Returns results with an ID greater than (more recent than)
                    the specified ID.
            max:	Returns results with an ID less than (older than) or
                    equal to the specified ID.
            limit:	Default and max limit is 30. This limit must be a
                    number under 30.
            callback:	Define your own callback function name,
                        add this parameter as the value.
            filter:	Filter messages by links, charts, videos,
                    or top. (Optional)
    """ 

    dictoinary_of_required_data = {
            "comments" : [],
            "date":[],
            "Timestamp":[],
            "Ticker" :[],
            "Watchlistcount": [],
            "userfollowing":[],
            "userfollowers":[],
            "userid" :[],
            "userideas":[],
            "userjoindate":[],
            "userlikecount":[],
            "userwatchliststockscount" :[],
            "linkssourcename":[],
            "linkstitle":[],
            "linksurl":[],
            "linksvideourl":[],
            "sentimentposition":[],
            "likestotal":[],
            "postid":[]
            }

    stock_counter = 1
    entry_counter = 0
    for stock in list_of_stocks:
        print(stock, ":", stock_counter)
        stock_counter += 1
        try:
            since_post_id = tracker.loc[stock, "since"]
            raw_json = twit.get_symbol_msgs(symbol_id=stock, since=since_post_id, max=0, limit=30, callback=None, filter=None)
            
            if(len(raw_json["messages"]) > 0):
                tracker.loc[stock, "since"] = raw_json["messages"][0]["id"]

            #Parsing the response into useful information 
            for post in raw_json["messages"]: 
                try:
                    #Setting up default values for all columns
                    dictoinary_of_required_data["comments"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["date"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["Timestamp"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["Ticker"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["Watchlistcount"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userfollowing"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userfollowers"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userid"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userideas"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userjoindate"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userlikecount"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["userwatchliststockscount"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["linkssourcename"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["linkstitle"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["linksurl"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["linksvideourl"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["sentimentposition"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["likestotal"].insert(entry_counter, np.nan)
                    dictoinary_of_required_data["postid"].insert(entry_counter, np.nan)
                    #Check for seeing if data already exists
                    if (post['id'] in dictoinary_of_required_data['postid'] ):
                        indexes =[index for index, value in enumerate(dictoinary_of_required_data['postid']) if value == post['id']]
                        Data_exists=  False
                        for _  in indexes:
                            if dictoinary_of_required_data['Ticker'][_] ==stock:

                                print(f"data already exists for {stock}")
                                Data_exists = True 
                                break
                        if Data_exists == True : 
                            break
                    #print(f'new data for {stock}')
                    dictoinary_of_required_data["comments"][entry_counter] = post["body"]
                    
                    try : 
                        dictoinary_of_required_data["sentimentposition"][entry_counter] = post["entities"]["sentiment"]["basic"]
                    except : 
                        dictoinary_of_required_data["sentimentposition"][entry_counter] = "None"
                    dictoinary_of_required_data["date"][entry_counter] = post["created_at"].split("T")[0]
                    dictoinary_of_required_data["Timestamp"][entry_counter] = post["created_at"].split("T")[1].replace("Z","")
                    for x in post["symbols"]:
                        if x["symbol"]==stock:
                            dictoinary_of_required_data['Ticker'][entry_counter] =  x['symbol']
                            dictoinary_of_required_data['Watchlistcount'][entry_counter] = x['watchlist_count']
                    dictoinary_of_required_data['userfollowers'][entry_counter] = post['user']['followers']
                    dictoinary_of_required_data['userfollowing'][entry_counter] = post['user']['following']
                    dictoinary_of_required_data['userideas'][entry_counter] = post['user']['ideas']
                    dictoinary_of_required_data['userid'][entry_counter] = post['user']['id']
                    dictoinary_of_required_data['userjoindate'][entry_counter] =  post['user']['join_date']
                    dictoinary_of_required_data['userlikecount'][entry_counter] =  post['user']['like_count']
                    dictoinary_of_required_data['userwatchliststockscount'][entry_counter] = post['user']['watchlist_stocks_count']
                    dictoinary_of_required_data['postid'][entry_counter] = post['id']
                    try :
                        dictoinary_of_required_data['likestotal'][entry_counter] = post['likes']['total']
                    # Sometimes data isn't present for such occassions add an empty value
                    except: 
                        dictoinary_of_required_data['likestotal'][entry_counter] = ""
                    try:
                        dictoinary_of_required_data['linkssourcename'][entry_counter] = post['links'][0]['source']['name']
                        dictoinary_of_required_data['linkstitle'][entry_counter] = post['links'][0]['title']
                        dictoinary_of_required_data['linksurl'][entry_counter] = post['links'][0]['url']
                        dictoinary_of_required_data['linksvideourl'][entry_counter] = post['links'][0]['video_url']
                    except: 
                        dictoinary_of_required_data['linkssourcename'][entry_counter] = ""
                        dictoinary_of_required_data['linkstitle'][entry_counter] = ""
                        dictoinary_of_required_data['linksurl'][entry_counter] = ""
                        dictoinary_of_required_data['linksvideourl'][entry_counter] = ""
                    entry_counter+=1
                except Exception as e:    
                    entry_counter+=1
                    continue

        except Exception as e:
            print(e)
            continue
    return pd.DataFrame(dictoinary_of_required_data)
        


if __name__ == "__main__":

    df_tickers = pd.read_csv("..\Tickers\TickerList.csv")
    post_id_tracker_filepath = "post_id_tracker.csv"
    if(os.path.exists(post_id_tracker_filepath)):
        df_tracker = pd.read_csv(post_id_tracker_filepath)
        if(df_tickers.shape[0] != df_tracker.shape[0]):
            new_tickers = df_tickers.iloc[df_tracker.shape[0]:,0].values.tolist()
            for each_new_ticker in new_tickers:
                df_tracker.loc[len(df_tracker)] = [each_new_ticker, 0]
            df_tracker.to_csv(post_id_tracker_filepath, index =  False)    
    else:
        df_tracker = df_tickers.copy()
        df_tracker['since'] = 0
        df_tracker.to_csv(post_id_tracker_filepath, index = False)
    
    #print("--------Starting the data collection process------")
    df_tracker = df_tracker.set_index("Symbol")
    current_datetime = datetime.now(timezone("EST"))
    current_date = current_datetime.date().strftime("%Y%m%d")
    current_time = current_datetime.time().strftime("%H:%M:%S")
    filename = f"{current_date}.csv"
    filepath = f"data/posts/{current_date}.csv"
    
    df = create_dataset(df_tickers['Symbol'], df_tracker)
    if(os.path.exists(filepath)):
        df_tracker = df_tracker.reset_index()
        df_tracker.to_csv(post_id_tracker_filepath, index = False)
        df.to_csv(filepath, mode = 'a', header=False, index =False)
    else:
        df_tracker = df_tracker.reset_index()
        df_tracker.to_csv(post_id_tracker_filepath, index = False)
        df.to_csv(filepath, index = False)
    
    try:        
        with engine.connect() as con:    
            no_of_rows_inserted = df.to_sql('StocktwitsPost', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {current_date} : {current_time}')
    except Exception as e:
        print(e)
        print(f"Error occured for: {current_date} : {current_time}")

    


