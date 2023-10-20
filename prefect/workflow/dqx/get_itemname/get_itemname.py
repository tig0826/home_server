#a   -*- coding: utf-8 -*-

import openpyxl
import pandas as pd
from prefect import flow, task
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
import time
from time import sleep
from prefect_github.repository import GitHubRepository
from prefect.blocks.system import Secret
import datetime


@task(retries=3)
def load_git_repo():
    github_repository_block = GitHubRepository.load("github-dqx-get-itemname")


@task(retries=3)
def login_dqx():
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
    time.sleep(1)
    driver.find_element(By.XPATH,'//*[@id="welcome_box"]/div[2]/a').click()
    time.sleep(0.5)
    driver.find_element(By.XPATH,'//*[@id="contentArea"]/div/div[2]/form/table/tbody/tr[2]/td[3]/a').click()
    # 検索
    time.sleep(2)
    return driver


@task(retries=10)
def search_item(driver, search_word):
    # 出品情報を格納するリスト
    exhibition_data = []
    #すべての文字で検索
    # 検索するアイテム名を格納
    print(f"search {search_word}")
    # 検索フォームに入力
    time.sleep(1)
    s = driver.find_element(By.XPATH, '//*[@id="searchword"]').clear()
    time.sleep(1)
    s = driver.find_element(By.XPATH, '//*[@id="searchword"]')
    time.sleep(1)
    s.send_keys(search_word)
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="searchBoxArea"]/form/p[2]/input').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="searchTabItem"]').click()
    time.sleep(1)
    print("find_elements item list")
    while(True):
        # 出品データの取得
        elements = driver.find_elements(By.TAG_NAME, 'tr')
        # リストに格納
        for elem in elements:
            exhibition_data.append(elem.text.split())
        if len(driver.find_elements(By.XPATH, '//*[@class="next"]')) > 0 :
            driver.find_element(By.XPATH, '//*[@class="next"]').click()
            time.sleep(2)
        else:
            break
    time.sleep(5)
    return exhibition_data


@task(retries=3)
def deduplicate_item_list(exhibition_data):
    # アイテム名の重複など無駄な要素を削除する
    exhibition_data = [i for i in exhibition_data if len(i) > 1 and i[0] != "アイテム名"]
    return exhibition_data


@task(retries=3)
def separate_soubi_dougu(exhibition_data):
    print('-- separate soubi and dougu ---')
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


@task(retries=3)
def save_to_postgresql(df_dougu, df_soubi):
    # 収集したデータを保存
    print('-- save to postgresql ---')
    connection_config = {
            "user": "tig",
            "password": "0826tattkmhr",
            "host": "postgresql.mynet.local",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connection_config))
    df_dougu.to_sql('dougu_name', con=engine, if_exists='replace', index=False)
    df_soubi.to_sql('soubi_name', con=engine, if_exists='replace', index=False)


@flow(log_prints=True)
def get_itemname():
    load_git_repo()
    driver = login_dqx()
    exhibition_data = []
    hiragana_exclude = ['ぁ', 'ぃ', 'ぅ', 'ぇ', 'ぉ', 'っ', 'ゃ', 'ゅ', 'ょ', 'ゎ', 'ゐ', 'ゑ']
    hiragana = [chr(i) for i in range(12353, 12436) if chr(i) not in hiragana_exclude]
    katakana_exclude = ['ァ', 'ィ', 'ゥ', 'ェ', 'ォ', 'ッ', 'ャ', 'ュ', 'ョ', 'ヮ' ]
    katakana = [chr(i) for i in range(12449, 12532+1) if chr(i) not in katakana_exclude]
    kanji = ["石", "玉", "剣", "盾", "杖", "弓", "券", "書", "天", "竜", "草", "竹", "超"]
    for search_word in hiragana:
        exhibition_data += search_item(driver, search_word)
    for search_word in katakana:
        exhibition_data += search_item(driver, search_word)
    for search_word in kanji:
        exhibition_data += search_item(driver, search_word)
    exhibition_data = deduplicate_item_list(exhibition_data)
    df_dougu, df_soubi = separate_soubi_dougu(exhibition_data)
    save_to_postgresql(df_dougu, df_soubi)
    driver.quit()


if __name__ == "__main__":
    # get_itemname.serve(name="get_itemname",cron="0 1 15 * *")
    get_itemname.serve(name="dqx-get-itemname")

