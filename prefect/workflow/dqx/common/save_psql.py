import pandas as pd
from prefect import task
from prefect.blocks.system import Secret
from sqlalchemy import create_engine

@task(name="save to postgresql", retries=5, retry_delay_seconds=5)
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
