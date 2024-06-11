from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def place_dcid(place_name:str):
    place_url = "https://datacommons.org/place"
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    driver.get(place_url)
    driver.maximize_window()
    place_autocomplete = driver.find_element(value='place-autocomplete')
    place_autocomplete.send_keys(place_name)
    place_autocomplete.send_keys(Keys.ARROW_DOWN)
    place_autocomplete.send_keys(Keys.ENTER)
    time.sleep(0.2)
    url = driver.current_url
    extracted_dcid = '/'.join(url.split('/')[-2:])
    return extracted_dcid
