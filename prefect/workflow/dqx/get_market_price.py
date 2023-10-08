from prefect import flow, task
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import re
import config
import openpyxl
import datetime

@task
def get_market_price():
    options = webdriver.ChrommeOptions()
    options.add_argument('--headless')

    driver = webdriver.Chrome(executable_path="chromedriver.exe", chrome_options)
    driver.get('https://hiroba.dqx.jp/sc/search/')

    time.sleep(3)

    s = driver.find_elemets_by_xpath('//*[@id="sqexid"]')
    s[0].send_keys(config.USERID)
    s = driver.find_elements()
    
