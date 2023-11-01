
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
from prefect_github.repository import GitHubRepository
from prefect.blocks.system import Secret
import datetime

@task(retries=3)
def load_git_repo():
    github_repository_block = GitHubRepository.load("github-dqx-get-itemname")


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
    service = Service(executable_path="chromedriver-mac-arm64/chromedriver")
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

def query_item_name(table_name):
    secret_block_postgresql_passwd = Secret.load("postgresql-tig-passwd")
    postgresql_passwd = secret_block_postgresql_passwd.get()
    connection_config = {
            "user": "tig",
            "password": postgresql_passwd,
            "host": "postgresql.mynet.local",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connection_config))
    df = pd.read_sql_table(table_name, engine)
    return df


@task(retries=10)
def search_item(driver, search_word):
    # 検索するアイテム名を格納
    print(f"search {search_word}")
    # 検索フォームに入力
    time.sleep(1)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'searchword')))
    except TimeoutException as e:
        driver.find_element(By.XPATH, '//*[@id="historyBack"]').click()
        logging.error(f"TimeoutException occurred: {e}. Element with ID 'searchword' was not found")
    s = driver.find_element(By.XPATH, '//*[@id="searchword"]').clear()
    time.sleep(1)
    s = driver.find_element(By.XPATH, '//*[@id="searchword"]')
    time.sleep(1.5)
    s.send_keys(search_word)
    time.sleep(1)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'searchBoxArea')))
    except TimeoutException as e:
        driver.find_element(By.XPATH, '//*[@id="historyBack"]').click()
        logging.error(f"TimeoutException occurred: {e}. Element with ID 'searchBoxArea' was not found")
    driver.find_element(By.XPATH, '//*[@id="searchBoxArea"]/form/p[2]/input').click()
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@class="searchItemTable itemsearch"]')))
        hash_url = driver.find_elements(By.XPATH, '//*[@class="searchItemTable itemsearch"]/tbody/tr[1]/th/table/tbody/tr[1]/td[3]/a')[0].get_attribute("href")
        hash_code = [i for i in hash_url.split("/") if i != ""]
        hash_code = hash_code [-1]
    except TimeoutException as e:
        hash_code = ""
    return hash_code

@task(retries=3)
def save_to_postgresql(df, table_name):
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
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


@flow(log_prints=True)
def get_item_hash():
    load_git_repo()
    table_list = ["name_weapon", "name_armor", "name_dougu"]
    for table_name in table_list:
        url = "https://hiroba.dqx.jp/sc/search/"
        driver = login_dqx(url)
        exhibition_data = []
        df = query_item_name(table_name)
        for _, row in df.iterrows():
            item_name = row["アイテム名"]
            item_category = row["カテゴリ"]
            hash_code = search_item(driver, item_name)
            exhibition_data.append([item_name, item_category, hash_code])
        df_hash = pd.DataFrame(exhibition_data, columns = ["アイテム名", "カテゴリ", "ハッシュ"])
        save_to_postgresql(df_hash, table_name)
        driver.quit()

if __name__ == "__main__":
    get_item_hash.serve(name="dqx-get-item-hash")


