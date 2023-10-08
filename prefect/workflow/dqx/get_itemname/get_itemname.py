#a   -*- coding: utf-8 -*-

import openpyxl
import pandas as pd
from prefect import Flow, task
from prefect.storage import Docker, Storage
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
import time
from time import sleep


@task(max_retries=3, retry_delay=datetime.timedelta(minutes=10))
def scrape_data():
    # 出品情報を格納するリスト
    exhibition_data = []
    options = webdriver.ChromeOptions()
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
    service = Service(executable_path="chromedriver-mac-arm64/chromedriver")
    driver = webdriver.Chrome(service=service,options=options)
    # 検索先のURL
    driver.get('https://hiroba.dqx.jp/sc/search/')
    time.sleep(1)
    # 検索窓入力
    s = driver.find_element(By.XPATH,'//*[@id="sqexid"]')
    s.send_keys('tatt08')
    s = driver.find_element(By.XPATH,'//*[@id="password"]')
    s.send_keys('4tattkmhr')
    # ログインボタンクリック
    driver.find_element(By.XPATH,'//*[@id="login-button"]').click()
    driver.find_element(By.XPATH,'//*[@id="welcome_box"]/div[2]/a').click()
    driver.find_element(By.XPATH,'//*[@id="contentArea"]/div/div[2]/form/table/tbody/tr[2]/td[3]/a').click()
    # 検索
    time.sleep(2)

    #すべての文字で検索
    hiragana = [chr(i) for i in range(12353, 12436)]
    for h in hiragana:
        # 検索するアイテム名を格納
        search_word = h
        # 検索フォームに入力
        s = driver.find_element(By.XPATH,'//*[@id="searchword"]').clear()
        s = driver.find_element(By.XPATH,'//*[@id="searchword"]')
        s.send_keys(search_word)
        driver.find_element(By.XPATH,'//*[@id="searchBoxArea"]/form/p[2]/input').click()
        time.sleep(0.5)
        driver.find_element(By.XPATH,'//*[@id="searchTabItem"]').click()
        time.sleep(0.5)
        while(True):
            # 出品データの取得
            elements = driver.find_elements(By.TAG_NAME,'tr')
            # リストに格納
            for elem in elements:
                exhibition_data.append(elem.text.split())
            if len(driver.find_elements(By.XPATH,'//*[@class="next"]')) > 0 :
                driver.find_element(By.XPATH,'//*[@class="next"]').click()
            else:
                break
        sleep(0.5)
    driver.quit()

    # 無駄な要素を削除
    exhibition_data = [i for i in exhibition_data if len(i) > 1 and i[0] != "アイテム名"]
    df_item = pd.DataFrame(exhibition_data)
    df_item = df_item.drop_duplicates().reset_index(drop=True)
    is_dougu = df_item.iloc[:, 4] == "装備可能な職業はありません"
    df_dougu = df_item[is_dougu].reset_index(drop=True)
    df_dougu = df_dougu.iloc[:, [0, 1]]
    df_dougu.columns = ["item_name", "category"]
    df_soubi = df_item[~ is_dougu].reset_index(drop=True)
    df_soubi = df_soubi.iloc[:, [0, 1, 3]]
    df_soubi.columns = ["item_name", "category", "level"]
    return df_dougu, df_soubi


@task
def save_to_postgresql(df_dougu, df_soubi):
    # 収集したデータを保存
    connection_config = {
            "user": "tig",
            "password": "0826tattkmhr",
            "host": "postgresql.mynet.local",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**connection_config))
    df_dougu.to_sql('dougu_name', con=engine, if_exists='replace', index=False)
    df_soubi.to_sql('soubi_name', con=engine, if_exists='replace', index=False)


# Flowの定義
with Flow("get_itemname") as flow:
    df_dougu, df_soubi = scrape_data()
    save_to_postgresql(df_dougu, df_soubi)

flow.run()
