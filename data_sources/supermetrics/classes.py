import requests

# from typing import Dict, List, Literal, Tuple
from pyspark.sql import SparkSession
from requests.exceptions import RequestException

# defines dbutils for the module
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class Supermetrics:
    def __init__(self) -> None:
        self.short_domain = ""

    def _set_response_structure(self, json_response):
        resp_keys = list(json_response.keys())
        resp_keys.sort()

        if "error" in resp_keys:
            raise RuntimeError(
                f"{json_response['error']['message']}: {json_response['error']['description']}"
            )
        if resp_keys == ["columns", "data", "fields", "notes"]:
            self.resp_structure = "columns,data,fields,notes"
            self.fields = json_response["fields"]
            self.status = json_response["notes"]["status"].lower()
        elif resp_keys == ["data", "meta"]:
            self.resp_structure = "data,meta"
            self.fields = json_response["meta"]["query"]["fields"]
            self.status = json_response["meta"]["status_code"].lower()
        else:
            raise TypeError(
                "Response does not match known format. Check URL is correct or map new json response structure."
            )
        self.data = json_response["data"]

    def get_data(self, url, params=None):
        if params:
            url = f"{self.short_url}/{url}"
        try:
            response = requests.get(url=url, params=params)
        except requests.exceptions.RequestException as e:
            raise e

        self._set_response_structure(response.json())

        return response.json()
