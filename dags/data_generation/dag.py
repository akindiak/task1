from http import HTTPMethod
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator

from data_generation.ops.generate_data import generate_orders_data

from common.open_exchange_api import OpenExchangeAUTH
from common.utils import bulk_load_data_into_postgres, remove_files
from constants import (
    COMMON_SQL_TEMPLATE_FOLDER,
    DATA_FOLDER,
    OPEN_EXCHANGE_CONN_ID,
    SOURCE_POSTGRES_CONN_ID,
)


GENERATED_DATA_PATH = (
        DATA_FOLDER / "generated_data" / "{{ ds }}" / "generated_orders_{{ ts_nodash }}.tsv"
).as_posix()

NUM_RECORDS_TO_GENERATE = 5000


with DAG(
    dag_id='data_generator_dag',
    description='This DAG generates orders data every 10 minutes and stores it on source database.',
    schedule="*/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    template_searchpath=[COMMON_SQL_TEMPLATE_FOLDER.as_posix()],
    is_paused_upon_creation=False,
) as dag:
    t1 = HttpOperator(
        task_id="retrieve_available_currencies",
        http_conn_id=OPEN_EXCHANGE_CONN_ID,
        method=HTTPMethod.GET,
        endpoint="/currencies.json",
        data={
            "show_alternative": False,
            "show_inactive": False,
        },
        auth_type=OpenExchangeAUTH,
        log_response=True,
        response_filter=lambda r: r.json()
    )

    t2 = PythonOperator(
        task_id='generate_orders_data',
        python_callable=generate_orders_data,
        op_kwargs={
            "save_to": GENERATED_DATA_PATH,
            "num_records": NUM_RECORDS_TO_GENERATE,
            "delta_days": 7,
        },
    )

    t3 = SQLExecuteQueryOperator(
        task_id='create_orders_table',
        conn_id=SOURCE_POSTGRES_CONN_ID,
        sql="create_orders_table.sql",
        params={
            "table_name": "orders",
        }
    )

    t4 = PythonOperator(
        task_id='load_orders_data_into_db',
        python_callable=bulk_load_data_into_postgres,
        op_kwargs={
            "conn_id": SOURCE_POSTGRES_CONN_ID,
            "table_name": "orders",
            "file_path": GENERATED_DATA_PATH,
        }
    )

    t5 = PythonOperator(
        task_id='remove_generated_orders_file',
        python_callable=remove_files,
        op_args={GENERATED_DATA_PATH},
    )

    t1 >> t2 >> t3 >> t4 >> t5
