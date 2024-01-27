import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
import datetime
from datetime import datetime, timedelta, timezone
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner

from common.save_psql import save_psql
from common.load_psql import load_psql
from common.login_dqx import login_dqx


def get_exhibit_price(session, item_name, item_type, item_category, item_hash, today, hour):
    print(f"-- search item name {item_name} ---")
    # 出品情報を格納するリスト
    data = []
    # 現在のページ番号
    pagenum = 0
    # 前回取得したページの情報の格納先
    last_page_content = ''
    while True:
        url = f"https://hiroba.dqx.jp/sc/search/bazaar/{item_hash}/page/{pagenum}"
        # ページの内容を取得
        sleep(3.1)
        target_response = session.get(url)
        if target_response.ok:
            page_content = target_response.text
        else:
            print("Failed to retrieve data with status code: ", target_response.status_code)
        soup = BeautifulSoup(target_response.content, 'html.parser')
        # 出品情報のテーブルが空の場合
        error_elements = soup.find_all(class_='txt_error')
        if len(error_elements) > 0:
            print(f"No items found for {item_name}")
            df = pd.DataFrame([], columns=["アイテム名",
                                           "取得日",
                                           "取得時刻",
                                           "種類",
                                           "カテゴリ",
                                           "できのよさ",
                                           "個数",
                                           "価格",
                                           "1つあたりの価格",
                                           "出品日",
                                           "出品期限",
                                           "取引相手"])
            return df
        # 出品情報のテーブルだけを抽出
        soup_tr = soup.find_all(class_='bazaarTable bazaarlist')[0]
        # 前のページと同じ内容なら終了
        if soup_tr == last_page_content:
            break
        # 現在のページ情報を格納
        last_page_content = soup_tr
        # 各行のデータを取得
        for row in soup_tr.find_all('tr')[1:]:  # 最初の行はヘッダーなのでスキップ
            cells = row.find_all('td')
            # アイテム名
            item_name = cells[0].find('a', class_='strongLnk').text.strip()
            # できのよさ
            quality = cells[0].find('span', class_='starArea').text.strip()
            # 個数
            count = int(cells[1].find_all('p')[0].text.split('：')[1].rstrip('こ'))
            # 価格と1つあたりの価格
            price_info = cells[1].find_all('p')[1].text.split('\n')
            price = price_info[0].split('：')[1].replace('G', '').strip()
            price = int(price)
            unit_price = price_info[1].replace('(ひとつあたり', '').replace('G)', '').strip()
            unit_price = int(unit_price) if unit_price != '' else None
            # 出品者
            seller = cells[1].find('a', class_='strongLnk').text.strip()
            # 出品期間
            period = cells[2].text.strip()
            exhibit_start, exhibit_end = period.split(' ～ ')
            # データをリストに追加
            data.append([item_name,
                         today,
                         hour,
                         item_type,
                         item_category,
                         quality,
                         count,
                         price,
                         unit_price,
                         exhibit_start,
                         exhibit_end,
                         seller])
        # ページ番号を増やす
        pagenum += 1
    df = pd.DataFrame(data, columns=["アイテム名",
                                     "取得日",
                                     "取得時刻",
                                     "種類",
                                     "カテゴリ",
                                     "できのよさ",
                                     "個数",
                                     "価格",
                                     "1つあたりの価格",
                                     "出品日",
                                     "出品期限",
                                     "取引相手"])
    return df


@task(name="get exhibit price",
      tags=["dqx", "dqx_price"],
      retries=5,
      retry_delay_seconds=5)
def get_price_split(session, df, today, hour):
    # 価格情報を取得から保存までを行うタスク
    # todayとhourを引数として渡しているのは、今回の処理が数時間かかっても、開始時刻を統一するため。
    schema_name = "price"  # 保存先のスキーマ名
    # 各行の要素を分解(アイテム名、カテゴリ、データベースのURLのハッシュ)
    item_name = df['アイテム名']
    item_type = df['種類']
    item_category = df['カテゴリ']
    item_hash = df['ハッシュ']
    try:
        # 相場情報データベースから検索
        df_price = get_exhibit_price(session,
                                     item_name,
                                     item_type,
                                     item_category,
                                     item_hash,
                                     today,
                                     hour)
    except Exception as e:
        print(f"スクレイピング時にエラーが発生しました: {e}")
    save_psql(df=df_price,
              table_name=item_name,
              schema_name=schema_name,
              if_exists='append')


@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_dougu():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    # postgresqlからアイテムのメタデータを取得
    df_dougu = load_psql("select * from metadata.item_name where 種類 = '道具'")
    # dqxのページにログイン
    session = login_dqx()
    for _, df in df_dougu.iterrows():
        # 出品情報を取得
        item_name = df['アイテム名']
        get_price_split.with_options(name=item_name).submit(session, df, today, hour)
    session.close()


@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_weapon():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    # postgresqlからアイテムのメタデータを取得
    df_weapon = load_psql("select * from metadata.item_name where 種類 = '武器'")
    # dqxのページにログイン
    session = login_dqx()
    for _, df in df_weapon.iterrows():
        # 出品情報を取得
        item_name = df['アイテム名']
        get_price_split.with_options(name=item_name).submit(session, df, today, hour)
    session.close()


@flow(log_prints=True, task_runner=DaskTaskRunner())
async def get_price_armor():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).strftime('%Y/%m/%d')
    hour = datetime.now(JST).strftime('%H')
    # postgresqlからアイテムのメタデータを取得
    df_armor = load_psql("select * from metadata.item_name where 種類 = '防具'")
    # dqxのページにログイン
    session = login_dqx()
    for _, df in df_armor.iterrows():
        # 出品情報を取得
        item_name = df['アイテム名']
        get_price_split.with_options(name=item_name).submit(session, df, today, hour)
    session.close()


if __name__ == "__main__":
    asyncio.run(get_price())

