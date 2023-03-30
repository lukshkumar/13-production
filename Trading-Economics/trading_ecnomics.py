from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time 
import pandas as pd 
from lxml import html
import requests
from html_table_parser.parser import HTMLTableParser
from datetime import datetime ,timedelta
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# THIS AREA MIGHT NEED MAINTANCE

def get_previous_week_data(url, next_month=False):
	"""Gets previous weeks data from trading ecnomics

	Arguments : 
	url : What url to crawl 
	next_month  : Wether to get data for next month 

	Note: Only pick ONE option at a time , if both are selected as False 
	 by defualt data for the previous week would be scraped
	"""
	

	# Setting up the driver for selenium
	chrome_options = Options()
	#We use headless mode because it is faster and does not require a GUI 
	chrome_options.add_argument("--headless")	
	driver = webdriver.Chrome('./chromedriver',options=chrome_options)
	driver.get(url)


	# all the driver.find_element_by_xpath code is for changing the options on the site to get right configuration 
	#There is a 5 second delay between each click to ensure the website has been loaded with the new information

	time.sleep(8)
	
	top_header_element = driver.find_element(By.CLASS_NAME,'btn-group-calendar')
	header_element = top_header_element.find_elements(By.CLASS_NAME,'btn-calendar')
	date_element = header_element[0]
	
	# ---- DATE ----
	date_element.click()
	time.sleep(4)
	date_elements = top_header_element.find_elements(By.CLASS_NAME,'te-c-option')

	if  next_month: 
		elem = date_elements[5] # 5 is the next month and 8 is the previous month
		elem.click()
		time.sleep(8)

	else : 
		elem = date_elements[4]
		elem.click()
		time.sleep(8)

	# ---- TIME ZONE UTC ----
	top_header_element = driver.find_element(By.CLASS_NAME,'btn-group-calendar')
	header_element = top_header_element.find_elements(By.CLASS_NAME,'btn-calendar')
	timezone_element = header_element[4]
	timezone_element.click()
	time.sleep(4)

	elem_options  = timezone_element.find_element(By.CLASS_NAME,'te-logic-timezone-select').find_elements(By.TAG_NAME,'option')
	for each_elem in elem_options:
		if each_elem.text == 'UTC -5':	
			each_elem.click()
			time.sleep(8)
			break

	# ---- 3 Star IMPACT ----
	top_header_element = driver.find_element(By.CLASS_NAME,'btn-group-calendar')
	header_element = top_header_element.find_elements(By.CLASS_NAME,'btn-calendar')
	impact_element = header_element[1]
	impact_element.click()
	time.sleep(4)
	elem_ul  = top_header_element.find_element(By.CLASS_NAME,'open').find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')
	elem_ul_star = elem_ul[2].find_element(By.TAG_NAME,'a')
	elem_ul_star.click()
	time.sleep(8)

	#Send the html to the parse function to make a csv
	raw_html = driver.page_source
	if next_month: 
		if datetime.now().date().month !=12:
			file_name = f"Data/{(datetime.now().date().month)+1}-{datetime.now().date().year}"
		else : 			
			file_name = f"Data/1-{(datetime.now().date().year)+1}"

	else: 

		file_name = f"Data/from_{(datetime.now()-timedelta(7)).date()}_to_{datetime.now().date()}"

	parse_html(raw_html,3,file_name)

	# ---- 2 Star IMPACT ----
	top_header_element = driver.find_element(By.CLASS_NAME,'btn-group-calendar')
	header_element = top_header_element.find_elements(By.CLASS_NAME,'btn-calendar')
	impact_element = header_element[1]
	impact_element.click()
	time.sleep(4)

	elem_ul  = top_header_element.find_element(By.CLASS_NAME,'open').find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')
	elem_ul_star = elem_ul[1].find_element(By.TAG_NAME,'a')
	elem_ul_star.click()
	time.sleep(8)

	#Send the html to the parse function to make a csv
	raw_html = driver.page_source
	parse_html(raw_html,2,file_name)

	# ---- 1 Star IMPACT ----
	top_header_element = driver.find_element(By.CLASS_NAME,'btn-group-calendar')
	header_element = top_header_element.find_elements(By.CLASS_NAME,'btn-calendar')
	impact_element = header_element[1]
	impact_element.click()
	time.sleep(4)

	elem_ul  = top_header_element.find_element(By.CLASS_NAME,'open').find_element(By.CLASS_NAME,'dropdown-menu').find_elements(By.TAG_NAME,'li')
	elem_ul_star = elem_ul[0].find_element(By.TAG_NAME,'a')
	elem_ul_star.click()
	time.sleep(8)

	#Send the html to the parse function to make a csv
	raw_html = driver.page_source
	parse_html(raw_html,1,file_name)

	driver.quit()


def parse_html(html,priority,name_of_File):
	"""Parses HTML  table into a pandas dataframe
	
	Arguments : 
	html : the html to parse . 
	priority : the priority of the data 
	name_of_File : the name of the csv that the data should be read from and written to. 
	"""
	p = HTMLTableParser()
	p.feed(html)
	try : 
		df = pd.read_csv(f"{name_of_File}.csv")
		DICTIONARY_OF_ALL_DATA = {
			"Day":list(df["Day"]),
			"Month":list(df["Month"]),
			"Year":list(df["Year"]),
			"Frequency":list(df["Frequency"]),
			"Date":list(df["Date"]),
			"Time":list(df["Time"]),
			"Country":list(df["Country"]),
			"Report":list(df["Report"]),
			"Priority":list(df["Priority"]),
			"Actual":list(df["Actual"]),
			"Previous":list(df["Previous"]),
			"Consensus":list(df["Consensus"]),
			"Forecast":list(df["Forecast"])
		}

	except : 
		DICTIONARY_OF_ALL_DATA = {
			"Day":[],
			"Month":[],
			"Year":[],
			"Frequency":[],
			"Date":[],
			"Time":[],
			"Country":[],
			"Report":[],
			"Priority":[],
			"Actual":[],
			"Previous":[],
			"Consensus":[],
			"Forecast":[]
		}
	date = None
	year = None
	month = None
	day = None 


	#Parsing the HTML and appending data to the dictionary 
	#Based on different lengths of the data we can classify what we have to do with it 

	for _ in p.tables[1:]: 

		if len(_) == 3: 
			for __ in _ : 
					if len(__) == 8 :
						DICTIONARY_OF_ALL_DATA['Report'].append(__[1])
						DICTIONARY_OF_ALL_DATA['Actual'].append(__[2])
						DICTIONARY_OF_ALL_DATA['Previous'].append(__[3])
						DICTIONARY_OF_ALL_DATA['Consensus'].append(__[4])
						DICTIONARY_OF_ALL_DATA['Forecast'].append(__[5])
						DICTIONARY_OF_ALL_DATA['Date'].append(date)
						DICTIONARY_OF_ALL_DATA['Year'].append(year)
						DICTIONARY_OF_ALL_DATA['Month'].append(month)
						DICTIONARY_OF_ALL_DATA['Day'].append(day)
						DICTIONARY_OF_ALL_DATA['Priority'].append(priority)
						DICTIONARY_OF_ALL_DATA['Frequency'].append("")
					elif len(__) == 3 : 
						DICTIONARY_OF_ALL_DATA['Time'].append(__[0])
						DICTIONARY_OF_ALL_DATA['Country'].append(__[2])
					elif len(__) ==7  : 
						data_of_date = __[0].split(' ')
						date = data_of_date[2] 
						year = data_of_date[3] 
						month = data_of_date[1] 
						day = data_of_date[0] 
					else : 
						print(_)

		elif len(_) ==2 : 
			DICTIONARY_OF_ALL_DATA['Report'].append(_[0][1])
			DICTIONARY_OF_ALL_DATA['Actual'].append(_[0][2])
			DICTIONARY_OF_ALL_DATA['Previous'].append(_[0][3])
			DICTIONARY_OF_ALL_DATA['Consensus'].append(_[0][4])
			DICTIONARY_OF_ALL_DATA['Forecast'].append(_[0][5])
			DICTIONARY_OF_ALL_DATA['Time'].append(_[1][0])
			DICTIONARY_OF_ALL_DATA['Country'].append(_[1][2])
			DICTIONARY_OF_ALL_DATA['Date'].append(date)
			DICTIONARY_OF_ALL_DATA['Year'].append(year)
			DICTIONARY_OF_ALL_DATA['Month'].append(month)
			DICTIONARY_OF_ALL_DATA['Day'].append(day)
			DICTIONARY_OF_ALL_DATA['Priority'].append(priority)
			DICTIONARY_OF_ALL_DATA['Frequency'].append("")
		elif len(_) ==4: 
			for __ in _ : 
					if len(__) == 8 :
						DICTIONARY_OF_ALL_DATA['Report'].append(__[1])
						DICTIONARY_OF_ALL_DATA['Actual'].append(__[2])
						DICTIONARY_OF_ALL_DATA['Previous'].append(__[3])
						DICTIONARY_OF_ALL_DATA['Consensus'].append(__[4])
						DICTIONARY_OF_ALL_DATA['Forecast'].append(__[5])
						DICTIONARY_OF_ALL_DATA['Date'].append(date)
						DICTIONARY_OF_ALL_DATA['Year'].append(year)
						DICTIONARY_OF_ALL_DATA['Month'].append(month)
						DICTIONARY_OF_ALL_DATA['Day'].append(day)
						DICTIONARY_OF_ALL_DATA['Priority'].append(priority)
						DICTIONARY_OF_ALL_DATA['Frequency'].append("")
					elif len(__) == 3 : 
						DICTIONARY_OF_ALL_DATA['Time'].append(__[0])
						DICTIONARY_OF_ALL_DATA['Country'].append(__[2])
					elif len(__) == 7 : 
						data_of_date = __[0].split(' ')
						date = data_of_date[2] 
						year = data_of_date[3] 
						month = data_of_date[1] 
						day = data_of_date[0] 
					else :
						print(_)
		elif len(_) ==1  : 
			DICTIONARY_OF_ALL_DATA['Report'].append(_[0][1])
			DICTIONARY_OF_ALL_DATA['Actual'].append(_[0][2])
			DICTIONARY_OF_ALL_DATA['Previous'].append(_[0][3])
			DICTIONARY_OF_ALL_DATA['Consensus'].append(_[0][4])
			DICTIONARY_OF_ALL_DATA['Forecast'].append(_[0][5])
			DICTIONARY_OF_ALL_DATA['Date'].append(date)
			DICTIONARY_OF_ALL_DATA['Year'].append(year)
			DICTIONARY_OF_ALL_DATA['Month'].append(month)
			DICTIONARY_OF_ALL_DATA['Day'].append(day)
			DICTIONARY_OF_ALL_DATA['Priority'].append(priority)
			DICTIONARY_OF_ALL_DATA['Frequency'].append("")
		else : 
			print(_)

	#Writing everything to a csv and removing duplicates .
	df  = pd.DataFrame(DICTIONARY_OF_ALL_DATA)
	df = df.drop_duplicates(['Day','Month','Year','Date', 'Time','Report','Country', 'Priority'],keep= 'last', ignore_index=True)
	df.to_csv(f"{name_of_File}.csv", index = False)




#Call to the main function , pass the website and if we want to get data for previous week 
#just pass the URL without any other arguments 

# get data for next month 
get_previous_week_data('https://tradingeconomics.com/calendar',True)


# get data for previous week 

#get_previous_week_data('https://tradingeconomics.com/calendar')
