from selenium import webdriver
import requests
from selenium.webdriver.chrome.options import Options
import json
import sys
sys.path.insert(1,'../')

f = open('../configurations.json')
 
# returns JSON object as a dictionary
configurations = json.load(f)
 
# Closing file
f.close()


year = configurations["ShortSqueeze"]["year"]
email = configurations["ShortSqueeze"]["email"]
password = configurations["ShortSqueeze"]["password"]



sign_in_url = "https://shortsqueeze.com/signin.php"

url = "https://shortsqueeze.com/" + year + ".php"	

# Setting up the driver for selenium
chrome_options = Options()
#We use headless mode because it is faster and does not require a GUI 
chrome_options.add_argument("--headless")	
driver = webdriver.Chrome('./chromedriver',options=chrome_options)
driver.get(sign_in_url)


# all the driver.find_element_by_xpath code is for changing the options on the site to get right configuration 
#There is a 7 second delay between each click to ensure the website has been loaded with the new information

# Login To ShortSqueeze
email_elem = driver.find_element_by_xpath('//*/div/table[5]/tbody/tr/td/div/table/tbody/tr/td/form/table/tbody/tr[1]/td[2]/input')
email_elem.send_keys(email)
password_elem = driver.find_element_by_xpath('//*/div/table[5]/tbody/tr/td/div/table/tbody/tr/td/form/table/tbody/tr[2]/td[2]/input')
password_elem.send_keys(password)
submit_elem = driver.find_element_by_xpath('//*/div/table[5]/tbody/tr/td/div/table/tbody/tr/td/form/table/tbody/tr[3]/td[2]/input')
submit_elem.click()

#time.sleep(5)

# Redirect to the page to download Excel Files
driver.get(url)

# The loop has to run 12 times as there are 12 rows (Jan - Jun (A and B)). If the <a> is available and the href has .xlsx at the last as extension 
# then that file is available to be downloaded as excel file. 
for i in range(24):
	filename = ""
	if(i <= 11):
		td_index = str(2)
		tr_index = str(i + 1)
	else:
		td_index = str(5)
		tr_index = str(i + 1 - 12)
	xpath_font = '//*/div/table[11]/tbody/tr/td[2]/div/table[4]/tbody/tr/td/div/table[2]/tbody/tr[' + tr_index + ']/td[' + td_index + ']/font'
	xpath_a = '//*/div/table[11]/tbody/tr/td[2]/div/table[4]/tbody/tr/td/div/table[2]/tbody/tr[' + tr_index + ']/td[' + td_index + ']/font/a'
	
	print(xpath_font)
	
	try:
		elem = driver.find_element_by_xpath(xpath_font)
		filename = str(elem.text)
		print("Filename: ", filename)
		
		elem_a = driver.find_element_by_xpath(xpath_a)

		filepath = str(elem_a.get_attribute("href"))
		print("Filepath: ", filepath)
		
		excel_filename_index = filepath.find("shortint.")
		excel_filename_long = filepath[excel_filename_index + 9:len(filepath) - 5]
		excel_filename_long_index = excel_filename_long.find("-")
		excel_filename = excel_filename_long[:excel_filename_long_index] + ".xlsx"
		print("Excel Filename: ", excel_filename)
		
		
		if(filepath[len(filepath) - 4 : len(filepath)] == "xlsx" or filepath[len(filepath) - 4 : len(filepath)] == "xlsm"):
			print("Downloading the Excel File.....")

			r = requests.get(filepath)
			#Save as Zip File
			with open("Data/" + excel_filename, "wb") as code:
				code.write(r.content)

			print("The Excel File '" + filename + "' is Downloaded Successfully!")

		else:
			print("The Excel File is Not Yet Ready to be Downloaded!")
			break
	except:
		print("The Excel File '" + filename + "' is Not Yet Ready to be Downloaded!")
		break

	