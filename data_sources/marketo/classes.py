import hashlib, logging, requests
from typing import Dict, List, Literal, Tuple
from pathlib import Path

import pendulum
from oauthlib.oauth2 import BackendApplicationClient
from pyspark.sql import SparkSession
from requests_oauthlib import OAuth2Session
from requests.exceptions import RequestException

from shared.functions.azure_utilities import get_key_vault_scope
from shared.functions.file_io import generate_unique_filename
from shared.classes import RESTBase

# defines dbutils for the module
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class MarketoREST(RESTBase):
    def __init__(
        self, client_id: str = None, client_secret: str = None, kv_scope: str = None
    ):
        self.domain = "https://649-DLI-726.mktorest.com"

        super().__init__(data_source="marketo")
        self._get_auth_credentials(client_id, client_secret, kv_scope)

    def _get_auth_credentials(
        self,
        manual_id: str = None,
        manual_secret: str = None,
        manual_kv_scope: str = None,
    ):
        self.kv_scope = manual_kv_scope or get_key_vault_scope()
        self.client_id = manual_id or dbutils.secrets.get(
            self.kv_scope, "marketo-api-client-id"
        )
        self.client_secret = manual_secret or dbutils.secrets.get(
            self.kv_scope, "marketo-api-client-secret"
        )

    def _get_bearer_token(self):
        token_url = f"{self.domain}//identity/oauth/token"

        client = BackendApplicationClient(client_id=self.client_id)
        oauth = OAuth2Session(client=client)
        response = oauth.fetch_token(
            token_url=token_url, client=client, client_secret=self.client_secret
        )
        self.token = response["access_token"]
        self.auth_header = {"Authorization": f"Bearer {self.token}"}

    def _assure_request_success(self, response: requests.Response):
        self.raise_for_status(response)

        if not response.json()["success"]:
            logging.error(response.json())
            raise ValueError('request "success" field returned `False`')

    def create_async_export_job(
        self,
        api_object: str,
        start_date: pendulum.Date,
        end_date: pendulum.Date,
        export_filter: Literal["createdAt", "updatedAt"],
        fields: Dict[Literal["fields"], List[str]] = {},
    ) -> str:
        if export_filter not in ["createdAt", "updatedAt"]:
            raise ValueError(
                f'Invalid export_filter, "{export_filter}". Expected one of ["createdAt", "updatedAt"]'
            )
        self._get_bearer_token()

        url = f"{self.domain}/bulk/v1/{api_object}/export/create.json"
        request_body = {
            **fields,
            "filter": {
                export_filter: {
                    "startAt": start_date.format("YYYY-MM-DD\THH:mm:ss\Z"),
                    "endAt": end_date.format("YYYY-MM-DD\THH:mm:ss\Z"),
                }
            },
        }
        response = requests.post(url, json=request_body, headers=self.auth_header)
        self._assure_request_success(response)

        logging.info(response.json())
        return response.json()["result"][0]["exportId"]

    def enqueue_async_export(self, api_object: str, job_id: str) -> None:
        self._get_bearer_token()

        url = f"{self.domain}/bulk/v1/{api_object}/export/{job_id}/enqueue.json"
        response = requests.post(url, headers=self.auth_header)
        self._assure_request_success(response)

    def check_async_export_status(
        self, api_object: str, job_id: str
    ) -> Tuple[str, str]:
        self._get_bearer_token()

        url = f"{self.domain}/bulk/v1/{api_object}/export/{job_id}/status.json"
        response = requests.get(url, headers=self.auth_header)
        self._assure_request_success(response)

        job_status = response.json()["result"][0]["status"]
        file_checksum = response.json()["result"][0].get("fileChecksum")

        return job_status, file_checksum

    def load_completed_job_to_raw(self, api_object: str, job_id: str):
        self._get_bearer_token()

        url = f"{self.domain}/bulk/v1/{api_object}/export/{job_id}/file.json"

        with requests.get(url, headers=self.auth_header, stream=True) as download:
            directory = Path(f"/dbfs/{self.storage_paths.landing}/{api_object}")
            directory.mkdir(exist_ok=True)
            filename = generate_unique_filename(api_object)
            filepath = f"{directory}/{filename}.csv"

            with open(filepath, "wb") as file:
                for chunk in download.iter_content(chunk_size=(10 * (1024**2))):
                    file.write(chunk)

        return filepath

    def verify_export_file_integrity(self, filepath: str, checksum: str):
        with open(filepath, "rb") as file:
            file_hash = f"sha256:{hashlib.sha256(file.read()).hexdigest()}"

        return file_hash == checksum

    def _get_paging_token(self, api_object: str, start_date: pendulum.Date):
        self._get_bearer_token()

        url = f"{self.domain}/rest/v1/{api_object}/pagingtoken.json"
        params = {"sinceDatetime": start_date.format("YYYY-MM-DD\THH:mm:ss\Z")}

        response = requests.get(url, params, headers=self.authorization_header)
        self._assure_request_success(response)

        return response.json()["nextPageToken"]

    def get_assets_data(
        self,
        api_object: str,
        start_date: pendulum.Date,
        max_return: int = 200,
        offset: int = 0,
    ) -> Tuple[List[dict], List[str], List[dict]]:
        self._get_bearer_token()

        url = f"{self.domain}/rest/asset/v1/{api_object}.json"
        params = {
            "earliestUpdatedAt": start_date.format(("YYYY-MM-DD\THH:mm:ss\Z")),
            "maxReturn": max_return,
            "offset": offset,
        }

        response = requests.get(url, params, headers=self.auth_header)
        self._assure_request_success(response)

        result = response.json().get("result") or []
        warnings = response.json().get("warnings") or []
        errors = response.json().get("errors") or []

        if warnings:
            logging.warning("For %s:" % api_object)
            logging.warning(*warnings)
        if errors:
            logging.warning("For %s:" % api_object)
            logging.error(**errors)
            raise RequestException

        return result, warnings, errors
