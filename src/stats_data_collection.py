from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.chrome.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from webdriver_manager.chrome import ChromeDriverManager
import concurrent.futures
import os
import json

with open('data_source_html.txt','r') as f:
    data_source_outer_html = f.read()
data_source_html = BeautifulSoup(data_source_outer_html,'lxml')

data_sources_dict = {}
for opt in data_source_html.find(class_="dataset-selector-custom-input custom-select").find_all('option'):
    opt_str = str(opt)
    opt_str = opt_str[opt_str.find("value=")+6:opt_str.find(">")][1:-1]
    data_source_link = "https://datacommons.org/tools/statvar#s="+"%2F".join(opt_str.split("/"))
    data_sources_dict.update({opt.text:data_source_link})

os.makedirs("STATS",exist_ok=True)

# data_sources_list = ['The New York Times','Brazil INPE - National Institute for Space Research','Google']
data_sources_list = list(data_sources_dict.keys())

curr_stats_files = []
for stats_files in os.listdir("STATS"):
    curr_stats_files.append(stats_files.split(".")[0])

# req_data_sources = [i for i in data_sources_list if i not in curr_stats_files]
req_data_sources = ["Wildland Fire Interagency Geospatial Services",'Wikimedia Foundation','U.S. National Wildfire Coordinating Group'
                    'U.S National Renewable Energy Laboratory (NREL)',
                    'U.S. National Oceanic and Atmospheric Administration (NOAA)',
                    'U.S. National Highway Traffic Safety Administration (NHTSA)',
                    'U.S. National Aeronautics and Space Administration (NASA)',
                    'U.S. Federal Reserve',
                    'U.S. Environmental Protection Agency (EPA)',
                    'U.S. Drug Enforcement Agency (DEA)',
                    'U.S. Department of Labor (DOL)',
                    'U.S. Department of Housing and Urban Development (HUD)',
                    'U.S. Department of Education (ED)',
                    'U.S. Department of Agriculture (USDA)',
                    'U.S. Bureau of Economic Analysis (BEA)',
                    'United States Geological Service (USGS)',
                    'Unique Identification Authority of India',
                    'UAE Bayanat Open Data Portal',
                    'Statistics of New Zealand',
                    'Stanford University',
                    'Stadt Zurich open data',
                    'Resources for the Future (RFF)',
                    'Our World in Data',
                    'India Water Resources Information System',
                    'India Ministry of Statistics and Programme Implementation',
                    'India Energy Dashboard',
                    'India Central Pollution Control Board (CPCB)',
                    'GeoSadak PMGSY National GIS, India',
                    'General Statistics Office (GSO), Vietnam',
                     'Food and Agriculture Organization of the United Nations',
                    'Feeding America',
                    'Federal Emergency Management Agency',
                    ]

# Define a function to recursively click elements with the class name 'title'
def recursively_click_titles(driver):
    # Using a set to keep track of clicked elements to avoid clicking the same element twice
    clicked_titles = set()

    def click_title_elements():
        # Find all elements with the class 'title'
        left_scroll = False
        elements = driver.find_elements(By.CLASS_NAME, 'title')
        for element in elements:
            # if element.tag_name !='span':
            #     continue
            # Check if the element has already been clicked to avoid re-clicking
            if element not in clicked_titles:
                try:
                    # Scroll into view and click the element
                    # ActionChains(driver).move_to_element(element).perform()
                    element.click()
                    
                    
                    # Add the element to the set of clicked titles
                    clicked_titles.add(element)
                    
                    # Wait for a moment to allow any dynamic content to load
                    actions = ActionChains(driver)

                    # Move the cursor away (e.g., 100 pixels to the right and down from the element)
                    if left_scroll:
                        actions.move_to_element_with_offset(element, 20, 20).perform()
                        left_scroll = False
                    else:
                        actions.move_to_element_with_offset(element, -20, -20).perform()
                        left_scroll = True
                    # Recursively call the function to handle new elements
                    click_title_elements()
                except Exception as e:
                    print(f"Error clicking element: {e}")
                    continue
    
    # Start the recursive clicking process
    click_title_elements()

def selenium_get_data(ds:str):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(data_sources_dict[ds])
    driver.maximize_window()
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "title"))
    )
    recursively_click_titles(driver)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    base_stats_link = "https://datacommons.org/browser/"
    node_titles = soup.find_all(class_="node-title")
    node_label_list = []
    for curr_node_title in node_titles:
        node_label = curr_node_title.find('label')
        if node_label is not None:
            node_label_str = str(node_label)
            node_label_str = node_label_str[node_label_str.find("for=")+4:node_label_str.find("/")][1:-2]
            node_label_list.append({"node_name":node_label.text,"node_dcid":node_label_str,"node_link":base_stats_link+node_label_str})
    save_path = os.path.join("STATS",f"{'_'.join(ds.split('.'))}.json")
    with open(save_path, 'w', encoding='utf-8') as json_file:
        json.dump(node_label_list, json_file, indent=4,ensure_ascii=True)
    print(f"Done for {ds}")

if __name__ == '__main__':
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(selenium_get_data,req_data_sources)