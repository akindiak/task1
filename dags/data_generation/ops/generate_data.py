import csv

from data_generation.ops.orders_generator import OrdersDataGenerator, GeneratedOrderRecord
from common.utils import create_parent_directories

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance


def generate_orders_data(save_to: str, num_records: int, delta_days: int, **kwargs) -> None:
    dag_run: DagRun = kwargs["dag_run"]
    ti: TaskInstance = kwargs["ti"]

    available_currencies = ti.xcom_pull(task_ids=["retrieve_available_currencies"])[0]

    orders_generator = OrdersDataGenerator(
        current_date=dag_run.logical_date,
        delta_days=delta_days,
        available_currencies=available_currencies,
    )

    create_parent_directories(path=save_to)

    with open(save_to, "w", newline="\n") as tsv_file:
        field_names = GeneratedOrderRecord.__annotations__.keys()
        writer = csv.DictWriter(tsv_file, delimiter="\t", fieldnames=field_names)

        for record in orders_generator.generate(num_records):
            writer.writerow(record)
