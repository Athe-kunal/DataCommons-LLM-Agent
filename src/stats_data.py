from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import concurrent.futures
from typing import List, Dict
import time
import yaml
import json

with open("config.yaml") as stream:
    try:
        config_params = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

def get_stats_tables(driver):
    stats_soup = BeautifulSoup(driver.page_source,"lxml")
    table_pages = stats_soup.find_all(class_="table-page-section")
    tbp = table_pages[1]
    cards = tbp.find_all(class_="card p-0")
    return cards

def get_properties(prop):
    node_table = prop.find(class_ ="node-table")
    trs = node_table.find_all('tr')

    properties_list = []
    for tr in trs:
        href_val = tr.find(class_="arc-text").find('a')['href']
        href_text = tr.find(class_="arc-text").text
        properties_list.append({"href":"https://datacommons.org"+href_val,"text":href_text})
    return properties_list

def get_dcids_from_table(stats):
    stats_vars_node_table = stats.find(class_="node-table")
    trs = stats_vars_node_table.find_all('tr')

    stats_vars_list = []
    for tr in trs:
        href_val = tr.find(class_="arc-text").find('a')['href']
        href_text = tr.find(class_="arc-text").text
        stats_desc = href_text[:href_text.find(("(dcid: "))].strip()
        stats_dcid = href_text[href_text.find(("dcid: "))+6:].strip()[:-1]
        stats_vars_list.append({"stats_link":"https://datacommons.org"+href_val,"stats_desc":stats_desc,"stats_dcid":stats_dcid})
    return stats_vars_list

def concurrent_worker_task(prop_elem):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(prop_elem['href'])
    driver.maximize_window()
    time.sleep(1)
    stats_soup = BeautifulSoup(driver.page_source,"lxml")
    table_pages = stats_soup.find_all(class_="table-page-section")
    tbp = table_pages[1]
    cards = tbp.find_all(class_="card p-0")

    card_dcids = []
    for card in cards:
        card_dcids.extend(get_dcids_from_table(card))
    return card_dcids

def get_dcids_from_properties(properties_list:List[Dict[str,str]]):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(concurrent_worker_task,properties_list)
    properties_dcids = []
    for res in results:
        properties_dcids.extend(res)
    return properties_dcids
    # properties_dcids = []
    # for prop_list in properties_list:
    #     properties_dcids.extend(concurrent_worker_task(prop_elem=prop_list))
    # return properties_dcids

def get_stats_vars_main_page():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    stats_url = "https://datacommons.org/browser/StatisticalVariable"
    driver.get(stats_url)
    driver.maximize_window()
    time.sleep(1)
    cards = get_stats_tables(driver)

    prop1 = cards[0]
    prop2 = cards[1]
    stats1 = cards[2]
    stats2 = cards[3]

    properties_list1 = get_properties(prop1)
    properties_list2 = get_properties(prop2)

    stats_dcids_1 = get_dcids_from_table(stats1)
    stats_dcids_2 = get_dcids_from_table(stats2)
    stats_dcids = stats_dcids_1 + stats_dcids_2

    return properties_list1,properties_list2,stats_dcids


def main_stats_scraper():
    properties_list1,properties_list2,stats_dcids = get_stats_vars_main_page()
    properties_dcids_1 = get_dcids_from_properties(properties_list1)
    properties_dcids_2 = get_dcids_from_properties(properties_list2)
    all_dcids = properties_dcids_1 + properties_dcids_2 + stats_dcids

    with open(
        config_params["DCID_SAVE_PATH"]["JSON_FILE_PATH"],
        "w",
        encoding="utf-8",
    ) as json_file:
        json.dump(all_dcids, json_file, ensure_ascii=True, indent=4)
