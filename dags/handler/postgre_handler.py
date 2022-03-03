import json
from loguru import logger
import pandas as pd
from sqlalchemy import create_engine

JSON_KEY_USER = "user"
JSON_KEY_PSWD = "password"
JSON_KEY_HOST = "host"
JSON_KEY_PORT = "port"

class PostgreHandler:
    
    # SINGLETON
    __instance = None
    def __new__(cls, *args, psql_creds):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls, *args)
        return cls.__instance

    def __init__(self, psql_creds):
        # Abre aquivo de config
        with open(psql_creds) as cfg:
            config = json.load(cfg)
            # Cria URL de conexao
            self.connection = create_engine(
                f'postgresql://{config[JSON_KEY_USER]}:{config[JSON_KEY_PSWD]}@{config[JSON_KEY_HOST]}:{config[JSON_KEY_PORT]}/')

        logger.success('Successfully Connected to Database')

    # Pega Dados do db
    def get_data(self, query):
        try:
            return pd.read_sql_query(query, self.connection)
        finally:
            self.connection.dispose()
    
    # Insere Dados no db 
    def insert_data(self, data, table_name):
        try:
            df = pd.DataFrame(data)
            df.to_sql(table_name, con=self.connection, if_exists='append', index=False)
            logger.success(f'Upload Success to {table_name}')
        finally:
            self.connection.dispose()


if __name__ == "__main__":
    #Teste Zone
    psql = PostgreHandler(psql_creds=r'C:\Users\israe\Desktop\desafio-stone\dags\configs\psql_creds.json')
    df = psql.get_data('SELECT * FROM product')
    print(df.head())

    # data = {'product_id': [4], 'name': 'Teste4'}
    # df = pd.DataFrame(data)
    # psql.insert_data(df, 'product')
    # logger.success("End of Execution")

    # ---- FIRST MOVE
    # from bigquery_handler import BigQueryHandle

    # bq_handle = BigQueryHandle(r'C:\Users\israe\Desktop\desafio-stone\dags\configs\gcp_creds.json')
    # psql = PostgreHandler(psql_creds=r'C:\Users\israe\Desktop\desafio-stone\dags\configs\psql_creds.json')

    # data = bq_handle.get_data(query="""
    # SELECT 
    #     * 
    # FROM 
    #     `bigquery-public-data.crypto_ethereum.tokens`
    # WHERE 
    #     block_timestamp 
    #     BETWEEN DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY) AND CURRENT_TIMESTAMP()
    # """)
    # print(data.head())
    # psql.insert_data(data, 'raw_tokens')