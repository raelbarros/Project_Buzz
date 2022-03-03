import json
from loguru import logger
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]

class BigQueryHandle(object):
    
    # --- LOGIN
    def __init__(self, key_path):
        logger.info("Connecting to BQ")
        self.client = self._auth_on_bq(key_path)

    def _auth_on_bq(self, key_path):
        try:
            credentials = service_account.Credentials.from_service_account_file(key_path, scopes=GCP_SCOPE, )
            bqclient = bigquery.Client(credentials=credentials, project=credentials.project_id)
            logger.success('Successfully Connected to BQ')
        except Exception as e:
            logger.exception(f'Failed to connect: {e}')
            raise

        return bqclient

    def get_data(self, query):
        logger.info(f"Executing query: {query}")
        data = (
            self.client.query(query)
            .result()
            .to_dataframe(create_bqstorage_client=True,)
        )
        return data


if __name__ == '__main__':
    #Teste Zone
    bq_handle = BigQueryHandle(r'C:\Users\israe\Desktop\desafio-stone\dags\configs\gcp_creds.json')

    data = bq_handle.get_data(query="""
    SELECT 
        * 
    FROM 
        `bigquery-public-data.crypto_ethereum.tokens`
    WHERE 
        block_timestamp 
        BETWEEN DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP()
    """)
    print(data.head())

    logger.success("End of Execution")