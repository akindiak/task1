import uuid
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Generator

from common.data_generator import BaseDataGenerator


@dataclass
class GeneratedOrderRecord:
    order_id: uuid
    customer_email: str
    order_date: datetime
    amount: float
    currency: str
    ingested_at: datetime


class OrdersDataGenerator(BaseDataGenerator):
    """Generator for orders data."""
    def __init__(
            self,
            current_date: datetime,
            delta_days: int,
            available_currencies: dict,
    ):
        self.current_date = current_date
        self.delta_days = delta_days
        self.available_currencies = available_currencies

    def generate(self, num_records: int) -> Generator[dict, None, None]:
        for i in range(num_records):
            record = GeneratedOrderRecord(
                order_id=self.uuid_gen(),
                customer_email=self.email_gen(),
                order_date=self.order_date_gen(self.current_date, self.delta_days),
                amount=self.amount_gen(),
                currency=self.currency_gen(self.available_currencies),
                ingested_at=self.current_date,
            )
            yield asdict(record)
