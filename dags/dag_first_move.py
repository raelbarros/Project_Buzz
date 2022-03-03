# Imports Airflow
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime

# Imports Handlers
from handler.bigquery_handler import BigQueryHandle
from handler.postgre_handler import PostgreHandler

import pendulum

# Variavel de timezone
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "onwer": "Israel Barros - <israel.silva.barros@hotmail.com>",
    'start_date': datetime(2022, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'concurrency': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='DAG_BQ_TO_PSQL_FIRST_MOVE',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15),
    catchup=False,
    tags=['FIRST', 'MOVE'],
)

# ---- FIRST MOVE
# Variavel de SELECT no Bigquery
QUERY_BQ = """
    SELECT 
        * 
    FROM 
        `bigquery-public-data.crypto_ethereum.tokens`
    WHERE 
        block_timestamp 
        BETWEEN DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY) AND DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    """

TABLE_PSQL = 'raw_tokens'

# Funcao de move dos dados entre BQ e PSQL
def move_bq_to_pqsl():
    # Inicializacao dos handles
    bq_handle = BigQueryHandle('/opt/airflow/dags/configs/gcp_creds.json')
    psql_handler = PostgreHandler(psql_creds='/opt/airflow/dags/configs/psql_creds.json') 

    # SELECT dados do BQ
    bq_data = bq_handle.get_data(query=QUERY_BQ)

    print(bq_data.head())

    # INSERT dados no PSQL
    psql_handler.insert_data(bq_data, TABLE_PSQL)

with dag:
    task_first_move = PythonOperator(
        task_id = "task_first_move",
        python_callable=move_bq_to_pqsl,
    )
    complete = DummyOperator(task_id='complete')

task_first_move >> complete