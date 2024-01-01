import pandas as pd
from prefect.blocks.system import Secret
from sqlalchemy import create_engine

def save_psql(df, table_name, schema_name, if_exists='append'):
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
    df.to_sql(table_name, con=engine, schema=schema_name, if_exists=if_exists, index=False)
