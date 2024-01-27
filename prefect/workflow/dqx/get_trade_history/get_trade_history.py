#a   -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import pandas as pd
from prefect import flow, task
import re
from time import sleep
import datetime
from datetime import datetime, timedelta

from common.save_psql import save_psql
from common.login_dqx import login_dqx


@task(retries=3, retry_delay_seconds=5)
def get_trade_buy(session):
    exhibition_data = []
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/purchasehistory/page/{pagenum}"
    pagenum = 0
    yesterday = (datetime.now()-timedelta(1)).strftime('%Y/%m/%d')
    trade_day = yesterday
    while trade_day == yesterday:
        sleep(2)
        target_response = session.get(url.format(pagenum=pagenum))
        if target_response.ok:
            page_content = target_response.text
        else:
            print("Failed to retrieve data with status code: ", target_response.status_code)
        soup = BeautifulSoup(target_response.content, 'html.parser')
        table = soup.find_all(class_="bazaarTable purchase")[0]
        rows = table.find_all('tr')
        for row in rows[1:]:
            sleep(0.1)
            cols = row.find_all('td')
            first_col = cols[0].text.strip()
            first_col_list = first_col.split('\n')
            if len(first_col_list) == 1:
                item_type = "どうぐ"
                item_name = first_col_list[0]
                item_quality = ""
            else:
                item_type = "そうび"
                item_name = first_col_list[0]
                item_quality = first_col_list[1].split("：")[1]
            item_count = int(cols[1].text.strip().replace('こ',''))
            item_price = int(cols[2].text.strip().replace(",", "").rstrip("G"))
            trade_day, trade_partner = cols[3].text.strip().split('\n')
            trade_day = trade_day.split("：")[1]
            trade_partner = trade_partner.split("：")[1]
            if trade_day == yesterday:
                exhibition_data.append([item_name,
                                        item_type,
                                        item_quality,
                                        item_count,
                                        item_price,
                                        trade_day,
                                        trade_partner])
        pagenum += 1
    df = pd.DataFrame(exhibition_data, columns=["アイテム名", "種類", "できのよさ", "個数", "価格", "取引日", "取引相手"])
    return df

@task(retries=3, retry_delay_seconds=5)
def get_trade_sell(session):
    exhibition_data = []
    url = "https://hiroba.dqx.jp/sc/character/484618740227/bazaar/entryhistory/page/{pagenum}"
    pagenum = 0
    yesterday = (datetime.now()-timedelta(1)).strftime('%Y/%m/%d')
    trade_day = yesterday
    while trade_day == yesterday:
        sleep(2)
        target_response = session.get(url.format(pagenum=pagenum))
        if target_response.ok:
            page_content = target_response.text
        else:
            print("Failed to retrieve data with status code: ", target_response.status_code)
        soup = BeautifulSoup(target_response.content, 'html.parser')
        table = soup.find_all(class_="bazaarTable entry")[0]
        rows = table.find_all('tr')
        for row in rows[1:]:
            sleep(0.1)
            cols = row.find_all('td')
            # アイテム名の列を取得
            first_col = cols[0].text.strip()
            # 道具以外の場合はアイテム名以外の情報が数行で含まている
            first_col_list = first_col.split('\n')
            if len(first_col_list) == 1:
                item_type = "どうぐ"
                item_name = first_col_list[0]
                item_quality = ""
            else:
                item_type = "そうび"
                item_name = first_col_list[0]
                item_quality = first_col_list[1].split("：")[1]
            # 取引した個数を取得
            item_count = int(cols[1].text.strip().replace('こ',''))
            # 取引した価格を取得
            item_price_text = cols[2].text.strip()
            # 正規表現を使用して数字のみを抽出
            item_price = re.findall(r'\d+', item_price_text.replace(",", ""))[0]
            item_price = int(item_price) if item_price != "-- " else 0
            forth_col_list = cols[3].text.strip().split('\n')
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
                if trade_day == yesterday:
                    exhibition_data.append([item_name, item_type, item_quality, item_count, item_price, trade_day, trade_partner])
        pagenum += 1
    df = pd.DataFrame(exhibition_data, columns=["アイテム名", "種類", "できのよさ", "個数", "価格", "取引日", "取引相手"])
    return df

@flow(log_prints=True)
def get_trade_history():
    schema_name = "trade_history"
    # 購入情報を取得
    session = login_dqx()
    df_buy = get_trade_buy(session)
    save_psql(df_buy, "buy_history", schema_name)
    # 売却情報を取得
    session = login_dqx()
    df_sell = get_trade_sell(session)
    save_psql(df_sell, "sell_history", schema_name)

if __name__ == "__main__":
    get_trade_history.serve(name="dqx-get-trade-history")

