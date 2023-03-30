from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
driver = webdriver.Chrome()

    # Navigate to url
driver.get("https://www.google.com/search?q=luksh&source=hp&ei=8Q6EYrC2FbSM9u8PovqJsAM&iflsig=AJiK0e8AAAAAYoQdAV1PNIb4QS3eQaqWTfOWB6hcmoPw&ved=0ahUKEwjw8ojouOf3AhU0hv0HHSJ9AjYQ4dUDCAc&uact=5&oq=luksh&gs_lcp=Cgdnd3Mtd2l6EAMyBQgAEIAEMgcIABCABBAKMg0ILhCABBDHARCvARAKMgoIABCxAxCDARAKMgQIABAKMhAILhCxAxCDARDHARCjAhAKMgcIABCxAxAKMgcIABCxAxAKMgQIABAKMgQIABAKOgsIABCABBCxAxCDAToRCC4QgAQQsQMQgwEQxwEQ0QM6CAgAEIAEELEDOgUIABCxAzoICAAQsQMQgwE6EQguEI8BENQCEOoCEIwDEOUCOg4IABCPARDqAhCMAxDlAjoICC4QgAQQsQM6CwguELEDEIMBENQCOgUILhCABDoLCC4QgAQQsQMQgwE6CAguEIAEENQCOgsILhCABBCxAxDUAjoKCC4QsQMQsQMQClAAWOIIYLUKaAJwAHgAgAHVAYgB_AeSAQUwLjEuNJgBAKABAbABCg&sclient=gws-wiz")

    # Enter "webdriver" text and perform "ENTER" keyboard action
#driver.find_element(By.NAME, "q").send_keys("webdriver" + Keys.ENTER)

    # Perform action ctrl + A (modifier CONTROL + Alphabet A) to select the page
#webdriver.ActionChains(driver).key_down(Keys.CONTROL).send_keys("a").perform()

submit_elem = driver.find_element_by_xpath('//*[@id="rso"]/div[1]/div/div/div[1]/div/a')

submit_elem.send_keys(Keys.CONTROL)
submit_elem.send_keys("F5")
submit_elem.click()