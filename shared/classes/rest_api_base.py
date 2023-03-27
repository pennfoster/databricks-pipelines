import logging, json
from requests import Response, HTTPError

from shared.classes import BaseClass


class RESTBase(BaseClass):
    def __init__(self, **kwargs):

        super().__init__(**kwargs)

    def raise_for_status(self, response: Response):
        try:
            response.raise_for_status()
        except HTTPError as e:
            logging.error(response.json())
            raise e
        else:
            if str(response.status_code) != "200":
                logging.warning("-- Response received other than 200 --")
                logging.warning(json.dumps(response.json(), indent=4))
