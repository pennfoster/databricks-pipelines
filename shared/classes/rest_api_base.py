import logging, time
from typing import Callable, TypeVar
from typing_extensions import ParamSpec

from aiohttp import ClientResponse, ClientResponseError
from requests import Response, HTTPError

from shared.classes import BaseClass

_P = ParamSpec("_P")
_T = TypeVar("_T")


class RESTBase(BaseClass):
    def __init__(self, **kwargs):
        self.retries = 3

        super().__init__(**kwargs)

    def raise_for_status(self, response: Response):
        try:
            response.raise_for_status()
        except HTTPError as e:
            raise e
        else:
            if str(response.status_code) != "200":
                logging.warning("-- Response received other than 200 --")
                logging.warning(response.text)

    async def async_raise_for_status(self, response: ClientResponse):
        try:
            response.raise_for_status()
        except ClientResponseError as e:
            raise e
        else:
            if str(response.status) != "200":
                logging.warning("-- Response received other than 200 --")
                logging.warning(await response.text())

    def retry(request_func: Callable[_P, _T]) -> Callable[_P, _T]:
        """Decorator for wrapping request functions that reliably return both a 429
        status code and a Retry-After header when enforcing rate limits. Defaults to 3
        retries - can be changed by adjusting the self.retry variable.
        """

        def _sleep(error, attempt_n, max_attempts, sleep_for):
            logging.warning(error)
            print(f"Attempt #{attempt_n} of {max_attempts} failed...")
            if attempt_n == max_attempts:
                raise error
            print(f"sleeping for {sleep_for} seconds...")
            time.sleep(int(sleep_for))
            print("retrying...")

        def _func(self, *args, **kwargs):
            for attempt in range(1, self.retries + 1):
                try:
                    response = request_func(self, *args, **kwargs)
                except HTTPError as e:
                    if str(e.response.status_code) == "429":
                        _sleep(
                            e, attempt, self.retries, e.response.headers["Retry-After"]
                        )
                        continue
                    raise e
                else:
                    return response

        _func.__wrapped__ = request_func
        return _func

    def async_retry(request_func: Callable[_P, _T]) -> Callable[_P, _T]:
        """Decorator for wrapping request functions that reliably return both a 429
        status code and a Retry-After header when enforcing rate limits. Defaults to 3
        retries - can be changed by adjusting the self.retry variable.
        """

        def _sleep(error, attempt_n, max_attempts, sleep_for):
            logging.warning(error)
            print(f"Attempt #{attempt_n} of {max_attempts} failed...")
            if attempt_n == max_attempts:
                raise error
            print(f"sleeping for {sleep_for} seconds...")
            time.sleep(int(sleep_for))
            print("retrying...")

        async def _func(self, *args, **kwargs):
            for attempt in range(1, self.retries + 1):
                try:
                    response = await request_func(self, *args, **kwargs)
                except ClientResponseError as e:
                    if str(e.status) == "429" and e.headers.get("Retry-After"):
                        _sleep(e, attempt, self.retries, e.headers["Retry-After"])
                        continue
                    raise e
                else:
                    return response

        _func.__wrapped__ = request_func
        return _func

    retry = staticmethod(retry)
    async_retry = staticmethod(async_retry)
