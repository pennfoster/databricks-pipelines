import asyncio
import logging
import requests
from typing import List, Dict, Tuple

import aiohttp
import nest_asyncio
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from shared.classes import RESTBase
from shared.functions.azure_utilities import get_key_vault_scope

nest_asyncio.apply()
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class LeadMantraREST(RESTBase):
    def __init__(self):
        self.domain = "https://chatbot.meera.ai/api/v1"

        super().__init__(data_source="lead_mantra")
        self._get_auth()

    def _get_auth(self, manual_token: str = None):
        self.token = manual_token or dbutils.secrets.get(
            get_key_vault_scope(), "lead-mantra-token"
        )
        self.auth_header = {"Authorization": self.token}

    def _assure_success(self, response: requests.Response):
        self.raise_for_status(response)
        assert response.json()["status"] == True
        assert response.json()["message"] == "Success"

    async def _assure_async_success(self, response: aiohttp.ClientResponse):
        await self.async_raise_for_status(response)
        j_response = await response.json()
        assert j_response["status"] == True
        assert j_response["message"] == "Success"

    def get_campaigns(self):
        response = requests.get(
            url=f"{self.domain}/campaign/lists",
            headers=self.auth_header,
        )
        self._assure_success(response)

        campaign_data = response.json()["campaign_data"]
        self.campaign_ids = [r["campaign_id"] for r in campaign_data]

        return campaign_data

    async def get_leads(
        self,
        campaign_id: str,
        start_date: str,
        passed_params: dict = {},
    ) -> List[dict]:
        url = f"{self.domain}/campaign-leads/get_data/{campaign_id}"
        params = {**passed_params, "filter_from_date": start_date}

        try:
            first_page, total_records, total_pages = self._get_leads_first_page(
                url, params
            )
        except requests.HTTPError as e:
            if e.response.json()["message"] == "no records found":
                logging.warning(
                    "No records found for campaign [%s] within search window."
                    % campaign_id
                )
                return []
            raise e
        else:
            remaining_pages = await self._get_leads_remaining_pages(
                url, params, total_pages
            )

            leads = [*first_page, *[row for page in remaining_pages for row in page]]
            assert len(leads) == total_records
            return leads

    async def _get_leads_remaining_pages(self, url, params, total_pages):
        async with aiohttp.ClientSession(
            headers=self.auth_header,
        ) as session:
            data = await asyncio.gather(
                *[
                    self._get_leads_page_coroutine(
                        session=session,
                        url=url,
                        page=n,
                        params=params,
                    )
                    for n in range(2, total_pages + 1)
                ]
            )

        return data

    def _get_leads_first_page(self, url, params) -> Tuple[requests.Response, int, int]:
        response = requests.get(url, headers=self.auth_header, params=params)
        self._assure_success(response)
        j_response = response.json()
        total_pages = j_response["total_pages"]
        total_records = j_response["total_record"]
        leads = j_response["campaign_leads"]
        return leads, total_records, total_pages

    async def _get_leads_page_coroutine(
        self,
        session: aiohttp.ClientSession,
        url: str,
        page: int,
        params: Dict[str, str],
    ):
        async with session.get(url, params={**params, "page": page}) as response:
            await self._assure_async_success(response)
            output = await response.json()
            return output["campaign_leads"]

    @RESTBase.async_retry
    async def get_lead_asset(
        self,
        endpoint: str,
        json_key: str,
        id_pairs: List[Tuple[int, int]],
    ):
        url = f"{self.domain}/{endpoint}"
        async with aiohttp.ClientSession(headers=self.auth_header) as session:
            results = await asyncio.gather(
                *[
                    self._lead_asset_coroutine(
                        session=session,
                        url=url,
                        json_key=json_key,
                        campaign_id=cid,
                        external_system_id=xid,
                    )
                    for cid, xid in id_pairs
                ]
            )
            return results

    async def _lead_asset_coroutine(
        self,
        session: aiohttp.ClientSession,
        url: str,
        json_key: str,
        campaign_id: str,
        external_system_id: str,
    ):
        request_body = {
            "campaign_id": campaign_id,
            "external_system_id": external_system_id,
        }
        try:
            async with session.post(url, json=request_body) as response:
                await self._assure_async_success(response)
                data = await response.json()
                output = data[json_key]
                if type(output) is dict:
                    return {**request_body, **output}
                if type(output) is list:
                    return [{**request_body, **item} for item in output]
        except aiohttp.ClientResponseError as e:
            print("campaign_id: ", campaign_id)
            print("external_system_id: ", external_system_id)
            raise e
