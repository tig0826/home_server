#a   -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy.util import warn_exception
from prefect import flow, task
import psycopg2
from sqlalchemy import create_engine
import time
from time import sleep
from prefect_github.repository import GitHubRepository
from prefect.blocks.system import Secret
import datetime
import requests
from bs4 import BeautifulSoup


@task(retries=3)
def search_item(base_url,url_hashes):
    item_list = []
    for item_type, u in url_hashes.items():
        item_name = ""
        pagenum=1
        while item_name != "No Results Found!":
            url = base_url.format(u=u,pagenum=pagenum)
            print(url)
            response = requests.get(url)
            response.encoding = response.apparent_encoding
            bs = BeautifulSoup(response.text, "html.parser")
            for row in bs.find_all('tr'):
                cells = row.find_all('td')
                item_name = cells[0].getText().strip()
                print(item_name)
                if item_name != "No Results Found!":
                    item_list.append([item_name,item_type])
            sleep(0.3)
            pagenum += 1
    df = pd.DataFrame(item_list, columns=["アイテム名", "カテゴリ"])
    return df

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
def get_itemname():
    weapon_base_url = "https://dqx-souba.game-blog.app/equip/{u}?min_shuppin=0&max_shuppin=600&max_lv=120&min_lv=1&sortColumn=lv&sortDirection=desc&page={pagenum}"
    weapon_url_hashes = {
            "片手剣" : "5efced945477a85bc95818712ee21a7ead69dc79",
            "両手剣" : "f5ae932a9a1c29df422fb1ace2fbf47b04fb8b22",
            "短剣" : "060670c72defc85aa5617558304d80709df433c4",
            "槍" : "6d74ee269f99d56a9cc6d232908d46db1616b627",
            "斧" : "e43b64fcb698a5a621df2804f9497d3b6de102da",
            "爪" : "62e63d700ac5f945f6fa7d5c186af99382da22ba",
            "鞭" : "f16e1c8dd6ec2edb5bbd71e7baab20b5d43bef0d",
            "スティック" : "8c62ace4fc6584e77043e067337ea47bcd1473d9",
            "両手杖" : "eac13cd87919b844bbd03c7e923962b924eed38c",
            "棍" : "2f2fdbed31f8efa5889adc07e21af697e069bb63",
            "扇" : "af17d80dc1b100c5cd5d013ec49d53e113c870e8",
            "ハンマー" : "aa5b1c0e7ffff8e14f305d298307d80a06cfdde7",
            "弓" : "d89b5a73d0fa96a0327b20bfa8a3b74aa1a8010c",
            "ブーメラン" : "52289762a19be43f6fca58ad5cf9529127421aaa",
            "鎌" : "c7784660f019b039a59c83e40bcd4264c31ea51f"
            }

    armor_base_url = "https://dqx-souba.game-blog.app/equip/{u}?min_shuppin=0&max_shuppin=600&max_lv=120&min_lv=1&sortColumn=lv&sortDirection=desc&page={pagenum}"
    armor_urls_hashes = {
            "頭" : "8b4ffbf6743e78e19dbd7907fddcff70822b6022",
            "体上" : "30212e6c5e9d731b0631927e340ec113487111c7",
            "体下" : "0b23ded64e49d3f61d356525c35d3ebd18fe9c2c",
            "腕" : "186ec38d3ee521c2b9cb930a15b46954553cabb5",
            "足" : "506c43307dfedececc21317de1eb870ec6690e88",
            "盾" : "bb2d8b75f236e4f753ac13d3d7f2ae698a307896"
            }


    dougu_base_url = "https://dqx-souba.game-blog.app/item/{u}?min_shuppin=0&max_shuppin=600&subcategory=all&sortColumn=lv&sortDirection=desc&bazaar=false&drop_item=false&n_drop_item=false&page={pagenum}"
    dougu_urls_hashes = {
            "素材" : "7aa9b827f1185a011e67191405869c2165447040",
            "消費アイテム" : "7459fb532a1d69a54091d951b2623242335bf46b",
            "印章" : "9b64c10654cd7a846b6b98cc415ca4e17b2c557b",
            "レシピ" : "1524543f24a0e6505e01f3409c0992258511d384",
            "料理" : "3323a1b5c53af813752fb6e9f253d6e3a7a2c014",
            "しぐさ書" : "7936501f8f4b40fbfc2c9112be7a96ca0398f43a",
            "スカウトの書" : "aa9d09cdd083b9bf07ff587a45d269707ff93920",
            "ポーズ書" : "a592f11567a3327e84a4fbb2dc225f2908dcc8c2",
            "タネ" : "31cd09a4fa3d2530b9437b44f9553bc3ee20afcd",
            "釣りざお" : "3da56db203ffa9c5b6eaebde15cb07622ccd4103",
            "ルアー" : "6c9eac07f2d64fec2d7eeee689000952beccf668"
            }

    df_weapon = search_item(weapon_base_url, weapon_url_hashes)
    df_armor = search_item(armor_base_url, armor_urls_hashes)
    df_dougu = search_item(dougu_base_url, dougu_urls_hashes)
    save_to_postgresql(df_weapon, "name_weapon")
    save_to_postgresql(df_armor, "name_armor")
    save_to_postgresql(df_dougu, "name_dougu")

if __name__ == "__main__":
    # get_itemname.serve(name="get_itemname",cron="0 1 15 * *")
    get_itemname.serve(name="dqx-get-itemname")

