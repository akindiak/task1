from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path


def remove_files(*files: str) -> None:
    for file in files:
        Path(file).unlink(missing_ok=True)


def bulk_load_data_into_postgres(conn_id: str, table_name: str, file_path: str):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)

    pg_hook.bulk_load(table_name, file_path)


def create_parent_directories(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
