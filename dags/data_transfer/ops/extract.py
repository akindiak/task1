from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.utils import create_parent_directories, remove_files
from datetime import datetime

from airflow.models.connection import Connection


def fetch_orders_data_from_postgres(conn_id: str, save_to: str, load_from: datetime):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = f"""
        SELECT
            order_id,
            customer_email,
            order_date,
            amount,
            currency
        FROM orders
        WHERE ingested_at > ('{ load_from }'::timestamp - interval '1 hour')
    """

    remove_files(save_to)
    create_parent_directories(path=save_to)

    write_header = True
    for df in pg_hook.get_pandas_df_by_chunks(sql, chunksize=5000):
        df.to_csv(save_to, index=False, sep="\t", mode="a", header=write_header)
        write_header = False
