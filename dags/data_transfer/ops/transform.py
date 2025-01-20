import pandas as pd
from common.utils import create_parent_directories, remove_files
from airflow.models.taskinstance import TaskInstance


def convert_orders_currencies(
        extracted_data_path: str,
        save_to: str,
        base_currency: str,
        **kwargs
) -> None:
    ti: TaskInstance = kwargs["ti"]
    convertion_rates = ti.xcom_pull(task_ids=['retrieve_exchange_rates'])[0].get("rates")

    remove_files(save_to)
    create_parent_directories(save_to)

    df = pd.read_csv(extracted_data_path, delimiter='\t')
    df["amount"] = df.apply(
        lambda row: row["amount"] / convertion_rates[row["currency"]], axis=1
    )
    df["currency"] = base_currency
    df["ingested_at"] = kwargs["logical_date"]

    df.to_csv(save_to, index=False, header=False, mode="w", sep="\t")
