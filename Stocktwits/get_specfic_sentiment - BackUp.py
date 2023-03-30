import requests
from bs4 import BeautifulSoup
import pandas as pd 
import json
from datetime import  datetime
import os
from pytz import timezone
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
import time as time_to_sleep

def get_information(stocks, today, time):

	"""Returns a dictionary of information about the given stocks 	
	Args: 
		stocks : List of stocks to get information about 
	"""

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

	for _ in stocks:
		main_url = f"https://stocktwits.com/symbol/{_}"
		# Setting up the driver for selenium
		chrome_options = Options()

		#Uncommit the below line to use headless mode because it is faster and does not require a GUI 
		#chrome_options.add_argument("--headless")	
		driver = webdriver.Chrome('./chromedriver',options=chrome_options)
		
		#Open the main page
		driver.get(main_url)
		time_to_sleep.sleep(5)
		#page_source = driver.page_source.split("</body>")[0].split("<body>")[-1]
		script_data = driver.find_elements(By.XPATH, "//*[@id='__NEXT_DATA__']")
		#script_data = driver.find_elements(By.XPATH, "/html/body/script[1]")
		#script_data = driver.find_element(By.TAG_NAME, "body").text.split("<script>")
		#script_data = driver.find_element(By.XPATH, "/html/body/")
		print(script_data.innerHTML)
		# page = requests.get(URL)
		# print(_,":",page)
		# soup = BeautifulSoup(page.content, "html.parser")
		# print(soup)
		# time = datetime.now().time()
		# #Parsing the raw HTML to get data 
		# for line in str(soup).split("\n"):
		# 	print(line)
		# 	#if "window.INITIAL_STATE" in line : 
		# 	data= json.loads(line.split("=")[1][:-1])
		# 	break
		# print(data)
		# #Appending found data to lists , if data is not present appending  0 
		# try : 
		# 	required_data['sentiment'].append(data['stocks']['inventory'][_]['sentimentChange'])
		# except: 
		# 	required_data['sentiment'].append(0)
		
		# try:
		# 	required_data['message_volume'].append(data['stocks']['inventory'][_]['volumeChange'])
		# except: 
		# 	required_data['message_volume'].append(0)
		
		# try :
		# 	required_data['price'].append(data['stocks']['inventory'][_]['percentChange'])
		# except:
		# 	required_data['price'].append(0)
		
		# required_data['Ticker'].append(_)
		# required_data['timestamp_scraped'].append(time)
		# required_data['Date_scraped'].append(today)
		
		# #Parsing HTML for updated time 
		# for line in str(soup).split("\n"): 
		# 	if ("->Updated" in line):
		# 		required_data['Date_updated'].append(" ".join(line.split("->Updated")[1].split("</div>")[0].split(" ")[:4]))
		# 		required_data['timestamp_updated'].append(" ".join(line.split("->Updated")[1].split("</div>")[0].split(" ")[4:]))
		# 		break
		# else : 
		# 	required_data['Date_updated'].append(0)
		# 	required_data['timestamp_updated'].append(0)

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
	#print(df.head())
	if(os.path.exists(filepath)):
		df.to_csv(filepath, mode = 'a', header=False, index =False)
	else:
		#Re-reading the file again on everyday basis so that we can keep on getting updated list.
		df.to_csv(filepath, index = False)
	

