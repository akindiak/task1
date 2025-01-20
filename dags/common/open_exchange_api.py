from requests.auth import AuthBase
from constants import OPEN_EXCHANGE_API_TOKEN
from requests import Request


class OpenExchangeAUTH(AuthBase):
    def __call__(self, request: Request) -> Request:
        request.headers["Authorization"] = f"Token {OPEN_EXCHANGE_API_TOKEN}"
        request.headers["accept"] = "application/json"
        return request
