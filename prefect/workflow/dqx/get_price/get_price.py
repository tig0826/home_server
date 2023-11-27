import asyncio
from prefect.events.schemas import Resource
import pandas as pd
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_dask.task_runners import DaskTaskRunner
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine
from time import sleep
import datetime
from datetime import datetime, timedelta, timezone


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
    prefs = {"profile.default_content_setting_values.notifications": 2}
    options.add_experimental_option("prefs", prefs)
    # webdriverのパスを指定
    service = Service(executable_path="chromedriver-linux64/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    # 検索先のURL
    try:
        driver.get(url)
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, 'sqexid')))
        secret_block_user = Secret.load("dqx-user")
        dqx_user = secret_block_user.get()
        secret_block_passwd = Secret.load("dqx-passwd")
        dqx_passwd = secret_block_passwd.get()
        # 検索窓入力
        s = driver.find_element(By.XPATH, '//*[@id="sqexid"]')
        s.send_keys(dqx_user)
        s = driver.find_element(By.XPATH, '//*[@id="password"]')
        s.send_keys(dqx_passwd)
        # ログインボタンクリック
        sleep(2)
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, 'login-button')))
        driver.find_element(By.XPATH, '//*[@id="login-button"]').click()
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, 'welcome_box')))
        driver.find_element(By.XPATH, '//*[@id="welcome_box"]/div[2]/a').click()
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, 'contentArea')))
        driver.find_element(By.XPATH, '//*[@id="contentArea"]/div/div[2]/form/table/tbody/tr[2]/td[3]/a').click()
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        driver.quit()
    finally:
        return driver

def get_exhibit_price(driver, item_name, item_type, item_category, item_hash, today, hour):
    print(f"-- search item name {item_name} ---")
    exhibition_data = []
    pagenum = 0
    url = "https://hiroba.dqx.jp/sc/search/bazaar/{item_hash}/page/{pagenum}"
    first_row_prev = ""
    driver.get(url.format(item_hash=item_hash, pagenum=pagenum))
    sleep(3)
    error_elements = driver.find_elements(By.XPATH, '//*[@id="bazaarList"]/form/div/div/p[@class="txt_error"]')
    if len(error_elements) > 0:
        print(f"No items found for {item_name}")
        return pd.DataFrame()
    table = driver.find_element(By.XPATH, '//*[@class="bazaarTable bazaarlist"]')
    rows = table.find_elements(By.TAG_NAME, 'tr')
    first_row = rows[1].text
    while first_row != first_row_prev:
        # 最後のページまで繰り返す(前のページと同じ内容なら最後のページ)
        sleep(2)
        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, 'td')
            # 一列目の要素を整形 アイテム名とできのよさ
            first_col = cols[0].text
            first_col_list = first_col.split('\n')
            item_name = first_col_list[0]
            item_quality = first_col_list[1].split("：")[1]
            # 二列目の要素を整形 出品数と値段と出品者
            second_col_list = cols[1].text.split('\n')
            if len(second_col_list) == 4:
                item_count, item_price, unit_price, trade_partner = second_col_list
            else:
                item_count, item_price, trade_partner = second_col_list
            item_count = item_count.split("：")[1].rstrip("こ")
            item_count = int(item_count)
            item_price = item_price.split("：")[1]
            item_price = item_price.replace(",", "").rstrip("G")
            item_price = int(item_price)
            trade_partner = trade_partner.split("：")[1]
            thrird_col_list = cols[2].text.split(' ～ ')
            exhibit_start, exhibit_end = thrird_col_list
            exhibition_data.append([item_name,today, hour, item_type, item_category, item_quality, item_count, item_price, exhibit_start, exhibit_end, trade_partner])
        # 次のページを取得
        pagenum += 1
        driver.get(url.format(item_hash=item_hash, pagenum=pagenum))
        table = driver.find_element(By.XPATH, '//*[@class="bazaarTable bazaarlist"]')
        rows = table.find_elements(By.TAG_NAME, 'tr')
        first_row_prev = first_row
        first_row = rows[1].text
    df = pd.DataFrame(exhibition_data, columns=["アイテム名", "取得日", "取得時刻", "種類", "カテゴリ", "できのよさ", "個数", "価格", "出品日", "出品期限", "取引相手"])
    return df

@task(name="load from postgresql", retries=5, retry_delay_seconds=5)
def load_from_postgresql(sql):
    # 収集したデータを保存
    print('-- load from postgresql ---')
    secret_block_postgresql_passwd = Secret.load("postgresql-tig-passwd")
    postgresql_passwd = secret_block_postgresql_passwd.get()
    connection_config = {
            "user": "tig",
            "password": postgresql_passwd,
            "host": "192.168.0.151",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connection_config))
    df = pd.read_sql(sql, con=engine)
    return df

def save_to_postgresql(df, table_name, schema_name):
    # 収集したデータを保存
    print(f'-- save to postgresql {schema_name}.{table_name} ---')
    secret_block_postgresql_passwd = Secret.load("postgresql-tig-passwd")
    postgresql_passwd = secret_block_postgresql_passwd.get()
    connection_config = {
            "user": "tig",
            "password": postgresql_passwd,
            "host": "192.168.0.151",
            "port": "5432",
            "dbname": "dqx"}
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connection_config))
    df.to_sql(table_name, con=engine, schema=schema_name, if_exists='append', index=False)


@task(name="wrap_task", retries=5, retry_delay_seconds=5)
def wrap_task_get_price(df, url, item_type, today, hour):
    schema_name = "price"
    df_price_all = pd.DataFrame(columns=["アイテム名", "取得日", "取得時刻", "種類", "カテゴリ", "できのよさ", "個数", "価格", "出品日", "出品期限", "取引相手"])
    driver = login_dqx(url)
    for _, row in df.iterrows():
        item_name, item_category, item_hash = row
        try:
            df_price = get_exhibit_price(driver, item_name, item_type, item_category, item_hash, today, hour)
            df_price_all = pd.concat([df_price_all, df_price], ignore_index = True)
        except Exception as e:
            print(f"エラーが発生しました: {e}")
            driver.quit()
    driver.quit()
    group_price = df_price_all.groupby('アイテム名')
    df_price_all_dict = {item_name: group_data for item_name, group_data in group_price}
    for item_name, df_price in df_price_all_dict.items():
        item_name = item_name.split("+")[0]
        save_to_postgresql(df_price, item_name, schema_name)

@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_weapon():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/"
    schema_name = "price"
    item_type = "武器"
    schema_name_item_name = "item_name" # 全アイテム名の入っているスキーマ名
    df_weapon = load_from_postgresql(f"select * from {schema_name_item_name}.name_weapon")
    group_weapon = df_weapon.groupby('カテゴリ')
    df_weapon_list = [group_data for _, group_data in group_weapon]
    for df in df_weapon_list:
        item_type = "武器"
        wrap_task_get_price.submit(df, url, item_type, today, hour)


@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_armor():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/"
    schema_name = "price"
    item_type = "防具"
    schema_name_item_name = "item_name" # 全アイテム名の入っているスキーマ名
    df_armor  = load_from_postgresql(f"select * from {schema_name_item_name}.name_armor")
    group_armor  = df_armor.groupby('カテゴリ')
    df_armor_list = [group_data for _, group_data in group_armor]
    for df in df_armor_list:
        item_type = "防具"
        wrap_task_get_price.submit(df, url, item_type, today, hour)

@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_dougu():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/"
    schema_name = "price"
    item_type = "道具"
    schema_name_item_name = "item_name" # 全アイテム名の入っているスキーマ名
    df_dougu  = load_from_postgresql(f"select * from {schema_name_item_name}.name_dougu")
    group_dougu  = df_dougu.groupby('カテゴリ')
    df_dougu_list = [group_data for _, group_data in group_dougu]
    for df in df_dougu_list:
        item_type = "道具"
        wrap_task_get_price.submit(df, url, item_type, today, hour)


@flow(log_prints=True, task_runner=DaskTaskRunner())
async def main():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/"
    schema_name_item_name = "item_name" # 全アイテム名の入っているスキーマ名
    df_weapon = load_from_postgresql(f"select * from {schema_name_item_name}.name_weapon")
    df_armor  = load_from_postgresql(f"select * from {schema_name_item_name}.name_armor")
    df_dougu  = load_from_postgresql(f"select * from {schema_name_item_name}.name_dougu")
    group_weapon = df_weapon.groupby('カテゴリ')
    group_armor  = df_armor.groupby('カテゴリ')
    group_dougu  = df_dougu.groupby('カテゴリ')
    df_weapon_list = [group_data for _, group_data in group_weapon]
    df_armor_list = [group_data for _, group_data in group_armor]
    df_dougu_list = [group_data for _, group_data in group_dougu]
    for df in df_weapon_list:
        item_type = "武器"
        wrap_task_get_price.submit(df, url, item_type, today, hour)
    for df in df_armor_list:
        item_type = "防具"
        wrap_task_get_price.submit(df, url, item_type, today, hour)
    for df in df_dougu_list:
        item_type = "道具"
        wrap_task_get_price.submit(df, url, item_type, today, hour)

if __name__ == "__main__":
    asyncio.run(main())

