import json, logging, requests
from typing import NamedTuple, List, Literal, Union
from hashlib import sha256

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope
from shared.classes import RESTBase


class Job(NamedTuple):
    id: int
    name: str
    settings: dict
    hash: str


class Orchestration(RESTBase):
    def __init__(self, **kwargs):
        self.env_domain = sc.getConf().get("spark.databricks.workspaceUrl")

        self._get_auth_headers()
        super().__init__(**kwargs)

    def _get_auth_headers(self):
        kv_scope = get_key_vault_scope()
        dbricks_access_token = dbutils.secrets.get(kv_scope, "cicd-access-token")

        self.dbricks_auth_header = {"Authorization": f"Bearer {dbricks_access_token}"}
        self.github_auth_header = {}

    def get_repo_jobs(
        self, repo: str, path_to_jobs: str, branch: str = "master"
    ) -> List[Job]:
        url = f"https://api.github.com/repos/pennfoster/{repo}/contents/{path_to_jobs}"

        with requests.Session() as s:
            s.headers = {
                **s.headers,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
            jobs_dir = s.get(url, params={"ref": branch})
            self.raise_for_status(jobs_dir)

            job_data = [
                json.loads(s.get(job["download_url"]).text) for job in jobs_dir.json()
            ]

        assert len(set([j["name"].lower() for j in job_data])) == len(job_data)

        # Databricks API returns field as float, UI returns field as integer.
        # This interfears with correct comparison hashing.
        for job in job_data:
            in_clusters = job["job_clusters"]
            out_clusters = []
            for c in in_clusters:
                try:
                    c["new_cluster"]["azure_attributes"]["spot_bid_max_price"] = float(
                        c["new_cluster"]["azure_attributes"]["spot_bid_max_price"]
                    )
                    out_clusters.append(c)
                except KeyError:
                    out_clusters.append(c)

            job["job_clusters"] = out_clusters

        return [
            Job(
                id=None,
                name=job["name"],
                settings=job,
                hash=sha256(
                    json.dumps(job, sort_keys=True).encode("utf-8")
                ).hexdigest(),
            )
            for job in job_data
        ]

    @RESTBase.retry
    def get_current_env_jobs(self) -> List[Job]:
        data = []
        with requests.Session() as s:
            s.headers = {
                **s.headers,
                "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}",
            }
            s.params = {"limit": 25, "offset": 0, "expand_tasks": "true"}
            while True:
                response = s.get(f"https://{self.env_domain}/api/2.1/jobs/list")
                data.extend(response.json()["jobs"])
                if not response.json()["has_more"]:
                    break
                s.params["offset"] = len(data)

        extant_jobs: List[Job] = []
        for job in data:
            if job["settings"].get("tags", {}).get("workflow") != "CI/CD":
                continue

            # Databricks Jobs API won't return this key if there are no settings.
            # Without this key the hash comparison won't function.
            if not job["settings"].get("webhook_notifications"):
                job["settings"]["webhook_notifications"] = {}

            extant_jobs.append(
                Job(
                    id=job["job_id"],
                    name=job["settings"]["name"],
                    settings=job["settings"],
                    hash=sha256(
                        json.dumps(job["settings"], sort_keys=True).encode("utf-8")
                    ).hexdigest(),
                )
            )

        self._assure_unique_job_names(extant_jobs)

        return extant_jobs

    def _assure_unique_job_names(self, job_list: List[Job]):
        unique_job_names = set([j.name.lower() for j in job_list])
        if len(unique_job_names) != len(job_list):
            job_count = {}
            for name in unique_job_names:
                job_count[name] = 0
            for job in job_list:
                job_count[job.settings["name"].lower()] += 1
            for k, v in job_count.items():
                if v > 1:
                    logging.error("%s jobs named %s" % (v, k))
            raise ValueError(
                "Two or more DBricks CI/CD jobs have non-unique names, which will break CI/CD flow."
            )

    @RESTBase.retry
    def get_most_recent_run_status(self, session: requests.Session, job_id: int):
        url = f"https://{self.env_domain}/api/2.1/jobs/runs/list"
        params = {
            "job_id": str(job_id),
            "limit": 1,
        }
        response = session.get(url, params=params)
        self.raise_for_status(response)

        run = response.json()["runs"][0]

        lcs: Literal["TERMINATED", "INTERNAL_ERROR", "SKIPPED", "RUNNING"]
        rs: Union[Literal["SUCCESS", "FAILED"], None]

        lcs = run["state"].get("life_cycle_state")
        rs = run["state"].get("result_state")

        return lcs, rs

    def get_runs_statuses(self, job_ids: List[int]):
        with requests.Session() as s:
            s.headers = {
                **s.headers,
                **self.dbricks_auth_header,
            }
            return [self.get_most_recent_run_status(s, job) for job in job_ids]

    def delete_job(self, job_id: int):
        url = f"https://{self.env_domain}/api/2.1/jobs/delete"

        response = requests.post(
            url,
            headers=self.dbricks_auth_header,
            json={"job_id": job_id},
        )
        self.raise_for_status(response)

    def update_job(self, job_id: int, job_settings: dict):
        url = f"https://{self.env_domain}/api/2.1/jobs/reset"
        request_body = {"job_id": job_id, "new_settings": job_settings}

        response = requests.post(
            url,
            headers=self.dbricks_auth_header,
            json=request_body,
        )
        self.raise_for_status(response)

    def create_job(self, job_settings: dict):
        url = f"https://{self.env_domain}/api/2.1/jobs/create"
        request_body = job_settings

        response = requests.post(
            url,
            headers=self.dbricks_auth_header,
            json=request_body,
        )
        self.raise_for_status(response)

        return response.json()["job_id"]
