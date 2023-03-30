from selenium import webdriver
import requests
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import json
import sys
import time
sys.path.insert(1,'../')

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()


email = configurations["Colab"]["email"]
password = configurations["Colab"]["password"]
url = configurations["Colab"]["url"]

# Setting up the driver for selenium
chrome_options = Options()
#We use headless mode because it is faster and does not require a GUI 
#chrome_options.add_argument("--headless")	
driver = webdriver.Chrome('./chromedriver',options=chrome_options)
driver.get(url)

time.sleep(5)

# Login To Colab
email_elem = driver.find_element_by_xpath('//*[@id="identifierId"]')
email_elem.send_keys(email)

submit_elem_email = driver.find_element_by_xpath('//*[@id="identifierNext"]/div/button')
submit_elem_email.click()

time.sleep(5)

password_elem = driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input')
password_elem.send_keys(password)

submit_elem = driver.find_element_by_xpath('//*[@id="passwordNext"]/div/button')
submit_elem.click()

time.sleep(5)
	
#submit_elem.send_keys(Keys.CONTROL)
#submit_elem.send_keys("F9")
#submit_elem.click()

webdriver.ActionChains(driver).key_down(Keys.CONTROL).send_keys(Keys.F9).perform()

try:
	restart_kernel = driver.find_element_by_class_name("restartruntime")
	confirm_dialog = driver.find_element_by_class_name("dialog-confirm")
	confirm_dialog.click()
	time.sleep(3)
	webdriver.ActionChains(driver).key_down(Keys.CONTROL).send_keys(Keys.F9).perform()
except:
	print("INFO: No Need to restart kernel.")




