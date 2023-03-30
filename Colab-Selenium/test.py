from selenium import webdriver
import requests
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains as A
import json
import sys
import time
sys.path.insert(1,'../')

# Setting up the driver for selenium
chrome_options = Options()
#We use headless mode because it is faster and does not require a GUI 
#chrome_options.add_argument("--headless")	
driver = webdriver.Chrome('./chromedriver',options=chrome_options)
driver.get("https://www.google.com/")

time.sleep(2)

#email_input = driver.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input')
#email_input.send_keys("csjdbcsd")

# Login To Colab
#email_elem = driver.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[3]/center/input[1]')

#email_input = driver.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input')
#email_input.send_keys("hello")
#email_input.send_keys(Keys.F5)
#driver.send_keys(Keys.F5)
#email_elem.click(Keys.F5)
#a=  A(driver)

#a.key_down(Keys.F5)
 
# Perform action ctrl + A (modifier CONTROL + Alphabet A) to select the page
webdriver.ActionChains(driver).key_down(Keys.CONTROL).send_keys(Keys.F5).perform()


