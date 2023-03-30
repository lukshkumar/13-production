import pandas as pd 
from pystocktwits import *
import datetime
import time

startlog = time.time()
date = datetime.date.today() 
twit = Streamer()
def create_dataset(list_of_stocks):
    """Createse dataframes from stocks

    Args : 
    list_of_stocks : A list of all the stocks(Strings)  need to get posts from 
    """ 
    for stock in list_of_stocks:
        raw_json = twit.get_symbol_msgs(symbol_id=stock, since=0, max=0, limit=30, callback=None, filter=None)

        try  : 
            data = pd.read_csv(f"{date}-high-priority-list.csv")
            dictoinary_of_required_data = {
            "comments" : list(data['comments']),
            "date":list(data["date"]),
            "Timestamp":list(data["Timestamp"]),
            "Ticker" :list(data["Ticker"]),
            "Watchlistcount":list(data["Watchlistcount"]),
            "userfollowing":list(data["userfollowing"]),
            "userfollowers":list(data["userfollowers"]),
            "userid" :list(data["userid"]),
            "userideas":list(data["userideas"]),
            "userjoindate":list(data["userjoindate"]),
            "userlikecount":list(data["userlikecount"]),
            "userwatchliststockscount" :list(data["userwatchliststockscount"]),
            "linkssourcename":list(data["linkssourcename"]),
            "linkstitle":list(data["linkstitle"]),
            "linksurl":list(data["linksurl"]),
            "linksvideourl":list(data["linksvideourl"]),
            "sentimentposition":list(data["sentimentposition"]),
            "likestotal":list(data["likestotal"]),
            "postid":list(data['postid'])
            }
        except : 
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
        #Parsing the response into useful information 
        for post in raw_json["messages"]: 
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
            print(f'new data for {stock}')
            dictoinary_of_required_data["comments"].append(post["body"])
            try : 
                dictoinary_of_required_data["sentimentposition"].append(post["entities"]["sentiment"]["basic"])
            except : 
                dictoinary_of_required_data["sentimentposition"].append("None")
            dictoinary_of_required_data["date"].append(post["created_at"].split("T")[0])
            dictoinary_of_required_data["Timestamp"].append(post["created_at"].split("T")[1].replace("Z",""))
            for x in post["symbols"]:
                if x["symbol"]==stock:
                    dictoinary_of_required_data['Ticker'].append(x['symbol'])
                    dictoinary_of_required_data['Watchlistcount'].append(x['watchlist_count'])
            dictoinary_of_required_data['userfollowers'].append(post['user']['followers'])
            dictoinary_of_required_data['userfollowing'].append(post['user']['following'])
            dictoinary_of_required_data['userideas'].append(post['user']['ideas'])
            dictoinary_of_required_data['userid'].append(post['user']['id'])
            dictoinary_of_required_data['userjoindate'].append(post['user']['join_date'])
            dictoinary_of_required_data['userlikecount'].append(post['user']['like_count'])
            dictoinary_of_required_data['userwatchliststockscount'].append(post['user']['watchlist_stocks_count'])
            dictoinary_of_required_data['postid'].append(post['id'])
            try :
                dictoinary_of_required_data['likestotal'].append(post['likes']['total'])
            # Sometimes data isn't present for such occassions add an empty value
            except: 
                dictoinary_of_required_data['likestotal'].append("")
            try:
                dictoinary_of_required_data['linkssourcename'].append(post['links'][0]['source']['name'])
                dictoinary_of_required_data['linkstitle'].append(post['links'][0]['title'])
                dictoinary_of_required_data['linksurl'].append(post['links'][0]['url'])
                dictoinary_of_required_data['linksvideourl'].append(post['links'][0]['video_url'])
            except: 
                dictoinary_of_required_data['linkssourcename'].append("")
                dictoinary_of_required_data['linkstitle'].append("")
                dictoinary_of_required_data['linksurl'].append("")
                dictoinary_of_required_data['linksvideourl'].append("")
        dataframe = pd.DataFrame(dictoinary_of_required_data)
        dataframe.to_csv(f'high_priority_list_{date}.csv')


def tess(current_data):
    """Test to see how much time has elasped
    Args: 
    current_data : Data to return if  1000 seconds since the last test have not passed"""
    global startlog
    if time.time() - 1000 > startlog:
        print('its been 1000 secs')
        startlog = time.time()
        return get_data()
    return current_data
def get_data():
    """Read data from csv and filter """
    df = pd.read_csv(f"specific_sentiment_{date}.csv")
    df = df[-df.nunique()['Ticker']-1:]
    print(df)
    high_priority_list = []
    for __ in df.iterrows():
        if(__[1]['message_volume']> 5 or __[1]['message_volume'] < -5):
            high_priority_list.append(__[1]['Ticker'])
    return high_priority_list



if __name__ == "__main__":  
    data = get_data()
    while True : 
        create_dataset(data)
        data = tess(data)