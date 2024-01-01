import pandas as pd
from prefect import task
from prefect.blocks.system import Secret
from sqlalchemy import create_engine

@task(name="load from postgresql", retries=5, retry_delay_seconds=5)
def load_psql(sql):
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
