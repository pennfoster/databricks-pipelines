import json
import requests

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from requests.exceptions import RequestException

from shared.classes.rest_api_base import RESTBase

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class Supermetrics(RESTBase):
    def __init__(self) -> None:
        self.fields = None
        self.status = None
        self.resp_structure = None

    def set_metadata(self, json_response: json) -> None:
        resp_keys = list(json_response.keys())
        resp_keys.sort()

        if resp_keys == ["columns", "data", "fields", "notes"]:
            self.resp_structure = "columns,data,fields,notes"
            self.fields = json_response["fields"]
            self.status = json_response["notes"]["status"].lower()
        elif resp_keys == ["data", "meta"]:
            self.resp_structure = "data,meta"
            self.fields = json_response["meta"]["query"]["fields"]
            self.status = json_response["meta"]["status_code"].lower()
        else:
            raise RuntimeError(
                "Response does not match known format. Check URL is correct or map new json response structure."
            )

    def get_data(self, url: str, params: dict = None) -> json:
        response = requests.get(url=url, params=params)
        response.raise_for_status()

        self.set_metadata(response.json())

        if self.status.upper() != "SUCCESS":
            raise ValueError(f"Response status of {self.status} was not successful.")
        if "error" in response.json():
            raise RuntimeError(
                f"{response.json()['error']['message']}: {response.json()['error']['description']}"
            )

        return response.json()

    # def get_keyjson(self, json_resp):
    #     if not self.fields:
    #         self.set_metadata(json_resp)

    #     data = json_resp["data"][1:]
    #     columns = [i["id"] for i in self.fields]

    #     keyjson = []
    #     for row in data:
    #         pairs = {}
    #         for k, v in zip(columns, row):
    #             pairs[k] = v
    #             keyjson.append(pairs)

    #     return keyjson

    # def get_custom_params_query(self, search_name):
    #     if "google" in search_name.lower():
    #         query = """
    #             customurlparameters:mkwid as mkwid
    #             , customurlparameters:dskeyname as mkwid
    #             , customurlparameters:pubcode as pubcode
    #             , customurlparameters:adkey as adkey
    #             , customurlparameters:dskeyword as dskeyword
    #         """
    #     elif "bing" in search_name.lower():
    #         query = """
    #         """

    #     return query
