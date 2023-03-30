from selenium import webdriver
import time 
import pandas as pd 
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from datetime import  datetime
import os
from pytz import timezone
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine

#Creating database connection
engine = create_engine('postgresql://postgres:postgres@ec2-18-141-177-116.ap-southeast-1.compute.amazonaws.com:5432/Project-Hamburg')

#Scrapes data and writes it to a csv
def get_data(current_date, current_time):
    
    main_url = "https://money.cnn.com/data/fear-and-greed/"
   # Setting up the driver for selenium
    chrome_options = Options()

    #Uncommit the below line to use headless mode because it is faster and does not require a GUI 
    chrome_options.add_argument("--headless")	
    driver = webdriver.Chrome('./chromedriver',options=chrome_options)

    required_data = { 
        "date" : "",
        "timestamp" : "",
        "now" : "",
        "now_sentiment" : "",
        "previous_close" : "",
        "previous_close_sentiment" : "",
        "week_ago" : "",
        "week_ago_sentiment" : "",
        "month_ago" : "",
        "month_ago_sentiment" : "",
        "year_ago" : "",
        "year_ago_sentiment" : ""
    }
    
    #Open the main page
    driver.get(main_url)
    data_index_label_class_name = "data-index-label"
    header = driver.find_element(By.CLASS_NAME, "market-tabbed-container__tab--1")
    required_data['now'] = header.find_element(By.CLASS_NAME, "market-fng-gauge__meter").get_attribute(data_index_label_class_name)
    required_data['now_sentiment'] = header.find_element(By.CLASS_NAME, "market-fng-gauge__dial-number-value").text
    
    previous_class = header.find_element(By.CLASS_NAME, "market-fng-gauge__historical-item--prevClose")
    week_class = header.find_element(By.CLASS_NAME, "market-fng-gauge__historical-item--weekClose")
    month_class = header.find_element(By.CLASS_NAME, "market-fng-gauge__historical-item--monthClose")
    year_class = header.find_element(By.CLASS_NAME, "market-fng-gauge__historical-item--yearClose")
    index_value_class_name = "market-fng-gauge__historical-item-index-value"
    
    required_data['previous_close'] = previous_class.get_attribute(data_index_label_class_name)
    required_data['previous_close_sentiment'] = previous_class.find_element(By.CLASS_NAME, index_value_class_name).text

    required_data['week_ago'] = week_class.get_attribute(data_index_label_class_name)
    required_data['week_ago_sentiment'] = week_class.find_element(By.CLASS_NAME, index_value_class_name).text

    required_data['month_ago'] = month_class.get_attribute(data_index_label_class_name)
    required_data['month_ago_sentiment'] = month_class.find_element(By.CLASS_NAME, index_value_class_name).text

    required_data['year_ago'] = year_class.get_attribute(data_index_label_class_name)
    required_data['year_ago_sentiment'] = year_class.find_element(By.CLASS_NAME, index_value_class_name).text
    
    required_data['date'] = current_date
    required_data['timestamp'] = current_time
  
    return pd.DataFrame(required_data, index=[0])
    

if __name__ == "__main__":
    
    current_datetime = datetime.now(timezone("EST"))
    current_date = current_datetime.date().strftime("%Y%m%d")
    current_time = current_datetime.time().strftime("%H:%M:%S")
    filename = f"{current_date}.csv"
    filepath = f"data/{current_date}.csv"
    
    df = get_data(current_date, current_time)
    
    if(os.path.exists(filepath)):
        df.to_csv(filepath, mode = 'a', header=False, index =False)
    else:
        df.to_csv(filepath, index = False)
    
    #Inserting data into database
    try:
        with engine.connect() as con:    
            df["date"] = df["date"].astype(str)
            no_of_rows_inserted = df.to_sql('CNNFearAndGreed', engine, if_exists = "append", index = False)
            print(f'{no_of_rows_inserted} rows inserted for {current_date} : {current_time}')
    except Exception as e:
        print(e)
        print(f"Error occured for: {current_date} : {current_time}")
