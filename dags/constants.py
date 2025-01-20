import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("../.env")

DATA_FOLDER = (Path(os.getenv("AIRFLOW_HOME")) / "data").resolve()
COMMON_SQL_TEMPLATE_FOLDER = (Path(__file__).parent / "common" / "sql").resolve()

OPEN_EXCHANGE_API_TOKEN = os.getenv("OPEN_EXCHANGE_API_TOKEN")

OPEN_EXCHANGE_CONN_ID = "open_exchange_rates_api"


SOURCE_POSTGRES_CONN_ID = "postgres_source_db"
TARGET_POSTGRES_CONN_ID = "postgres_target_db"
