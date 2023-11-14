
#a   -*- coding: utf-8 -*-

import openpyxl
import logging
import pandas as pd
from prefect import flow, task
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine
import time
from time import sleep
# from prefect_github.repository import GitHubRepository
from prefect.blocks.system import Secret
#from prefect.run_config import KubernetesRun
import datetime
from datetime import datetime


@task(retries=3)
def login_dqx(url):
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("start-maximized")
    options.add_argument("enable-automation")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument('--disable-extensions')
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-browser-side-navigation")
    options.add_argument("--disable-gpu")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    prefs = {"profile.default_content_setting_values.notifications" : 2}
    options.add_experimental_option("prefs",prefs)
    # webdriverのパスを指定
    service = Service(executable_path="chromedriver-linux64/chromedriver")
    driver = webdriver.Chrome(service=service,options=options)
    # 検索先のURL
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'sqexid')))
    secret_block_user = Secret.load("dqx-user")
    dqx_user = secret_block_user.get()
    secret_block_passwd = Secret.load("dqx-passwd")
    dqx_passwd = secret_block_passwd.get()
    # 検索窓入力
    s = driver.find_element(By.XPATH,'//*[@id="sqexid"]')
    s.send_keys(dqx_user)
    s = driver.find_element(By.XPATH,'//*[@id="password"]')
    s.send_keys(dqx_passwd)
    # ログインボタンクリック
    driver.find_element(By.XPATH,'//*[@id="login-button"]').click()
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'welcome_box')))
    driver.find_element(By.XPATH,'//*[@id="welcome_box"]/div[2]/a').click()
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'contentArea')))
    driver.find_element(By.XPATH,'//*[@id="contentArea"]/div/div[2]/form/table/tbody/tr[2]/td[3]/a').click()
    # 検索
    return driver

@task(retries=3)
def get_trade_buy(driver):
    exhibition_data = []
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/purchasehistory/page/{pagenum}"
    pagenum = 0
    today = datetime.now().strftime('%Y/%m/%d')
    trade_day = today
    while trade_day == today:
        sleep(2)
        driver.get(url.format(pagenum=pagenum))
        table = driver.find_element(By.XPATH, '//*[@class="bazaarTable purchase"]')
        rows = table.find_elements(By.TAG_NAME, 'tr')
        for row in rows[1:]:
            sleep(0.1)
            cols = row.find_elements(By.TAG_NAME, 'td')
            first_col = cols[0].text
            first_col_list = first_col.split('\n')
            if len(first_col_list) == 1:
                item_type = "どうぐ"
                item_name = first_col_list[0]
                item_quality = ""
            else:
                item_type = "そうび"
                item_name = first_col_list[0]
                item_quality = first_col_list[1].split("：")[1]
            item_count = cols[1].text
            item_price = int(cols[2].text.replace(",", "").rstrip("G"))
            trade_day, trade_partner = cols[3].text.split('\n')
            trade_day = trade_day.split("：")[1]
            trade_partner = trade_partner.split("：")[1]
            if trade_day == today:
                exhibition_data.append([item_name, item_type, item_quality, item_count, item_price, trade_day, trade_partner])
        pagenum += 1
    df = pd.DataFrame(exhibition_data, columns=["アイテム名", "種類", "できのよさ", "個数", "価格", "取引日", "取引相手"])
    return df

@task(retries=3)
def get_trade_sell(driver):
    exhibition_data = []
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/page/{pagenum}"
    pagenum = 0
    today = datetime.now().strftime('%Y/%m/%d')
    trade_day = today
    while trade_day == today:
        driver.get(url.format(pagenum=pagenum))
        table = driver.find_element(By.XPATH, '//*[@class="bazaarTable entry"]')
        rows = table.find_elements(By.TAG_NAME, 'tr')
        for row in rows[1:]:
            sleep(0.1)
            cols = row.find_elements(By.TAG_NAME, 'td')
            first_col = cols[0].text
            first_col_list = first_col.split('\n')
            if len(first_col_list) == 1:
                item_type = "どうぐ"
                item_name = first_col_list[0]
                item_quality = ""
            else:
                item_type = "そうび"
                item_name = first_col_list[0]
                item_quality = first_col_list[1].split("：")[1]
            item_count = cols[1].text
            item_price = cols[2].text.split("\n")[0].replace(",", "").rstrip("G")
            item_price = int(item_price) if item_price != "-- " else 0
            forth_col_list = cols[3].text.split('\n')
            if len(forth_col_list) == 2:
                # 出品取り消し
                trade_day, trade_result = forth_col_list
                trade_day = trade_day.split("：")[1]
            elif len(forth_col_list) == 3:
                # 返却
                trade_day, trade_result, recieve_day = forth_col_list
                trade_day = trade_day.split("：")[1]
            elif len(forth_col_list) == 4:
                trade_day, trade_result, trade_partner, recieve_day = forth_col_list
                trade_day = trade_day.split("：")[1]
                trade_partner = trade_partner.split("：")[1]
                if trade_day == today:
                    exhibition_data.append([item_name, item_type, item_quality, item_count, item_price, trade_day, trade_partner])
        pagenum += 1
    df = pd.DataFrame(exhibition_data, columns=["アイテム名", "種類", "できのよさ", "個数", "価格", "取引日", "取引相手"])
    return df

@task(retries=3)
def save_to_postgresql(df, table_name, schema_name):
    # 収集したデータを保存
    print('-- save to postgresql ---')
    secret_block_postgresql_passwd = Secret.load("postgresql-tig-passwd")
    postgresql_passwd = secret_block_postgresql_passwd.get()
    connection_config = {
            "user": "tig",
            "password": postgresql_passwd,
            "host": "postgresql.mynet.local",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connection_config))
    df.to_sql(table_name, con=engine, schema=schema_name, if_exists='append', index=False)


@flow(log_prints=True)
def get_trade_history():
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/"
    schema_name = "trade_history"
    # 購入情報を取得
    driver = login_dqx(url)
    df_buy = get_trade_buy(driver)
    driver.quit()
    save_to_postgresql(df_buy, "buy_history", schema_name)
    # 売却情報を取得
    driver = login_dqx(url)
    df_sell = get_trade_sell(driver)
    driver.quit()
    save_to_postgresql(df_sell, "sell_history", schema_name)

if __name__ == "__main__":
    get_trade_history.serve(name="dqx-get-trade-history")


