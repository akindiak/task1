from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from data_transfer.ops.extract import fetch_orders_data_from_postgres
from data_transfer.ops.transform import convert_orders_currencies

from common.utils import bulk_load_data_into_postgres, remove_files
from common.open_exchange_api import OpenExchangeAUTH
from constants import (
    COMMON_SQL_TEMPLATE_FOLDER,
    DATA_FOLDER,
    SOURCE_POSTGRES_CONN_ID,
    TARGET_POSTGRES_CONN_ID,
    OPEN_EXCHANGE_CONN_ID,
)
from datetime import datetime
from http import HTTPMethod

EXTRACTED_DATA_PATH = (
        DATA_FOLDER / "transfer_data" / "{{ ds }}" / "fetched_orders_{{ ts_nodash }}.tsv"
).as_posix()

TRANSFORMED_DATA_PATH = (
        DATA_FOLDER / "transfer_data" / "{{ ds }}" / "transformed_orders_{{ ts_nodash }}.tsv"
).as_posix()

BASE_CURRENCY = "EUR"


with DAG(
    dag_id='data_transfer_dag',
    description="This Dag extracts orders data from source db, transforms it and loads into target DB",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    template_searchpath=[COMMON_SQL_TEMPLATE_FOLDER.as_posix()],
    is_paused_upon_creation=False,
) as dag:

    t1 = HttpOperator(
        task_id="retrieve_exchange_rates",
        http_conn_id=OPEN_EXCHANGE_CONN_ID,
        auth_type=OpenExchangeAUTH,
        method=HTTPMethod.GET,
        endpoint="/latest.json",
        data={
            "base": BASE_CURRENCY,
            "show_alternative": True,
        },
        log_response=True,
        response_filter=lambda r: r.json()
    )

    t2 = PythonOperator(
        task_id="extract_orders_data",
        python_callable=fetch_orders_data_from_postgres,
        op_kwargs={
            "conn_id": SOURCE_POSTGRES_CONN_ID,
            "save_to": EXTRACTED_DATA_PATH,
            "load_from": "{{ ts }}"
        },
    )

    t3 = PythonOperator(
        task_id="transform_orders_data",
        python_callable=convert_orders_currencies,
        op_kwargs={
            "extracted_data_path": EXTRACTED_DATA_PATH,
            "save_to": TRANSFORMED_DATA_PATH,
            "base_currency": BASE_CURRENCY
        },
    )

    t4 = SQLExecuteQueryOperator(
        task_id='create_staging_orders_table',
        conn_id=TARGET_POSTGRES_CONN_ID,
        sql="create_orders_table.sql",
        params={
            "table_name": "stg_orders_eur",
        }
    )

    t5 = SQLExecuteQueryOperator(
        task_id='create_target_orders_table',
        conn_id=TARGET_POSTGRES_CONN_ID,
        sql="create_orders_table.sql",
        params={
            "table_name": "orders_eur",
        }
    )

    t6 = PythonOperator(
        task_id="load_orders_data_into_db",
        python_callable=bulk_load_data_into_postgres,
        op_kwargs={
            "conn_id": TARGET_POSTGRES_CONN_ID,
            "table_name": "stg_orders_eur",
            "file_path": TRANSFORMED_DATA_PATH,
        }
    )

    t7 = SQLExecuteQueryOperator(
        task_id="upsert_orders_table",
        conn_id=TARGET_POSTGRES_CONN_ID,
        sql="upsert_orders_table.sql",
        params={
            "target_table": "orders_eur",
            "staging_table": "stg_orders_eur"
        }
    )

    t8 = SQLExecuteQueryOperator(
        task_id="delete_staging_orders_table",
        conn_id=TARGET_POSTGRES_CONN_ID,
        sql="drop_table.sql",
        params={
            "table_name": "stg_orders_eur"
        }
    )

    t9 = PythonOperator(
        task_id='remove_orders_artifacts',
        python_callable=remove_files,
        op_args={EXTRACTED_DATA_PATH, TRANSFORMED_DATA_PATH}
    )

    t1 >> t2 >> t3 >> [t4, t5] >> t6 >> t7 >> t8 >> t9
