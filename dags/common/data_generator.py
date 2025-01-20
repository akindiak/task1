import uuid
import random
import string
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import Generator


class BaseDataGenerator(ABC):

    @abstractmethod
    def generate(self, num_records: int) -> Generator[dict, None, None]:
        raise NotImplementedError

    @staticmethod
    def uuid_gen() -> uuid:
        return uuid.uuid4()

    @staticmethod
    def email_gen() -> str:
        prefix = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
        return f"{prefix}@example.com"

    @staticmethod
    def amount_gen(decimal_places: int = 2) -> float:
        return round(random.uniform(10, 1000), decimal_places)

    @staticmethod
    def order_date_gen(current_date: datetime, delta_days: int) -> datetime:
        from_date = current_date - timedelta(days=delta_days)
        random_seconds = random.randrange(int(from_date.timestamp()), int(current_date.timestamp()) + 1)
        return datetime.fromtimestamp(random_seconds)

    @staticmethod
    def currency_gen(available_currencies: dict) -> str:
        _ = available_currencies.pop("VEF", None)  # there is no exchange rate for this one
        return random.choice(list(available_currencies.keys()))
