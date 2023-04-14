import requests
from bs4 import BeautifulSoup
import pandas as pd 
import json
from datetime import  datetime
import os
from pytz import timezone
import json
from sqlalchemy import create_engine

#Create a connection to the database
def read_database_connection_string():
    text_file = open(r"D:/Project Hamburg/13-production/db-connection-string.txt", "r")
    CONNECTION_STRING = text_file.read()
    text_file.close()
    return CONNECTION_STRING

engine = create_engine(read_database_connection_string())

def get_information(stocks, today, time):

	#For Testing purpose. Uncomment the below line to test the code.
	#stocks = ["CDNS"]

	required_data = {
	"Ticker":[],
	'Date_updated':[],
	"timestamp_updated":[],
	'Date_scraped':[],
	'timestamp_scraped':[],
	'sentiment':[],
	'price':[],
	'message_volume':[],
	}

	stock_counter = 0
	for _ in stocks:
		try:
			required_data['Ticker'].insert(stock_counter, _)
			required_data['timestamp_scraped'].insert(stock_counter, time)
			required_data['Date_scraped'].insert(stock_counter, today)
			required_data['sentiment'].insert(stock_counter, "N/A")
			required_data['message_volume'].insert(stock_counter, "N/A")
			required_data['price'].insert(stock_counter, "N/A")
			required_data['Date_updated'].insert(stock_counter, "N/A")
			required_data['timestamp_updated'].insert(stock_counter, "N/A")
		
			url = f"https://stocktwits.com/symbol/{_}"

			# Make a request to the website and get the HTML
			html = requests.get(url).text

			# Parse the HTML using Beautiful Soup
			soup = BeautifulSoup(html, "html.parser")
			data = json.loads(soup._most_recent_element)
			#print(data)
			
			#sentimentChange
			try:
				required_data['sentiment'][stock_counter] = data["props"]["pageProps"]["initialData"]["sentimentChange"]
			except:
				pass

			#volumeChange
			try:
				required_data['message_volume'][stock_counter] = data["props"]["pageProps"]["initialData"]["volumeChange"]
			except:
				pass
			
			#percentChange
			try:
				required_data['price'][stock_counter] = data["props"]["pageProps"]["initialData"]["priceData"]["percentChange"]
			except:
				pass
			
			#lastUpdated
			try:
				updated_datetime = data["props"]["pageProps"]["initialData"]["lastUpdated"].split(',')
				required_data['Date_updated'][stock_counter] = updated_datetime[0]
				required_data['timestamp_updated'][stock_counter] = updated_datetime[1].strip()	
			except:
				pass
	
			stock_counter += 1

		except Exception as e:
			#print(e)
			stock_counter += 1
			continue	
	
	return pd.DataFrame(required_data)
	

if __name__ == "__main__":

	df_tickers = pd.read_csv("..\Tickers\TickerList.csv")
	#print("------------------------------------------------------")
	current_datetime = datetime.now(timezone("EST"))
	current_date = current_datetime.date().strftime("%Y%m%d")
	current_time = current_datetime.time().strftime("%H:%M:%S")
	filename = f"{current_date}.csv"
	filepath = f"data/sentiments/{current_date}.csv"

	df = get_information(df_tickers['Symbol'], current_date, current_time)
	if(os.path.exists(filepath)):
		df.to_csv(filepath, mode = 'a', header=False, index =False)
	else:
		#Re-reading the file again on everyday basis so that we can keep on getting updated list.
		df.to_csv(filepath, index = False)

	try:
		with engine.connect() as con:    
			df["Date_scraped"] = df["Date_scraped"].astype(str)
			no_of_rows_inserted = df.to_sql('StocktwitsSentiment', engine, if_exists = "append", index = False)
			print(f'{no_of_rows_inserted} rows inserted for {current_date} : {current_time}')
	except Exception as e:
		print(e)
		print(f"Error occured for: {current_date} : {current_time}")


