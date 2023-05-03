import json, logging, requests, re
from typing import NamedTuple, List, Literal, Union
from hashlib import sha256

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope
from shared.functions.databricks_utilities import _get_dbricks_env
from shared.classes import RESTBase


class Job(NamedTuple):
    id: int
    name: str
    settings: dict
    hash: str


class Orchestration(RESTBase):
    def __init__(self, **kwargs):
        self.env_domain = sc.getConf().get("spark.databricks.workspaceUrl")
        self.github_headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.github_domain = (
            "https://api.github.com/repos/pennfoster/databricks-pipelines"
        )

        self._get_auth_headers()
        super().__init__(**kwargs)

    def _get_auth_headers(self):
        kv_scope = get_key_vault_scope()

        dbricks_access_token = dbutils.secrets.get(kv_scope, "cicd-access-token")
        github_access_token = dbutils.secrets.get(kv_scope, "github-access-token")

        self.dbricks_auth_header = {"Authorization": f"Bearer {dbricks_access_token}"}
        self.github_headers = {
            **self.github_headers,
            "Authorization": f"Bearer {github_access_token}",
        }

    def get_repo_jobs(
        self, repo: str, path_to_jobs: str, branch: str = "master"
    ) -> List[Job]:
        url = f"https://api.github.com/repos/pennfoster/{repo}/contents/{path_to_jobs}"

        with requests.Session() as s:
            s.headers = {**s.headers, **self.github_headers}
            job_files = s.get(url, params={"ref": branch})
            self.raise_for_status(job_files)

            job_settings = [
                json.loads(s.get(file["download_url"]).text)
                for file in job_files.json()
            ]

        # TODO: Move this to a _helper method
        for job in job_settings:
            # Jobs not in production should be paused
            if job.get("schedule"):
                if _get_dbricks_env() != "prd":
                    job["schedule"]["pause_status"] = "PAUSED"
            # Databricks API returns field as float, UI returns field as integer.
            # This interfears with correct comparison hashing.
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

        repo_jobs: List[Job] = [
            Job(
                id=None,
                name=job["name"],
                settings=job,
                hash=sha256(
                    json.dumps(job, sort_keys=True).encode("utf-8")
                ).hexdigest(),
            )
            for job in job_settings
        ]

        self._assure_unique_job_names(repo_jobs)
        return repo_jobs

    @RESTBase.retry
    def get_current_env_jobs(self) -> List[Job]:
        data = []
        with requests.Session() as s:
            s.headers = {
                **s.headers,
                **self.dbricks_auth_header,
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
            # Skips any jobs without an explicit "CI/CD" tag
            if job["settings"].get("tags", {}).get("workflow") != "CI/CD":
                continue

            # Databricks Jobs API won't return this key if there are no settings.
            # Without this key the hash comparison doesn't work
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

        runs = response.json().get("runs")
        lcs: Literal["NEVER_RUN", "TERMINATED", "INTERNAL_ERROR", "SKIPPED", "RUNNING"]
        rs: Union[Literal["SUCCESS", "FAILED"], None]

        if runs:
            lcs = runs[0]["state"].get("life_cycle_state")
            rs = runs[0]["state"].get("result_state")
        else:
            lcs = "NEVER_RUN"
            rs = None

        return job_id, (lcs, rs)

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

    def create_release(self, head_branch_name):
        existing_release = None
        head_sha = self._get_branch_sha(head_branch_name)

        # TODO detect branch diff

        extant_pr_branches = self.get_extant_pull_request_head_refs()
        release_prs = [ref for ref in extant_pr_branches if ref.startswith("release")]
        if release_prs:
            if len(release_prs) > 1:
                raise ValueError(
                    "Multiple release pull requests pending."
                    " These must be manually resolved before CI/CD flow can continue"
                )
            release_name = release_prs[0]
            existing_release = True
        else:
            releases = self.get_branches()
            release_name = self.iterate_semantic_release_name(releases, "major")
            existing_release = False

        release_ref = f"refs/heads/{release_name}"

        # create or update relase branch
        if existing_release:
            self.update_branch(head_sha, release_ref)
            print(f"PR updated for {release_name}")
        else:
            self.create_branch(head_sha, release_ref)
            self.create_pull_request(release_name, release_name, "master")
            print(f"New PR created for {release_name}")

    def create_branch(self, git_sha, branch_ref) -> requests.Response:
        url = f"{self.github_domain}/git/refs"
        request_body = {"sha": git_sha, "ref": branch_ref}

        response = requests.post(url, headers=self.github_headers, json=request_body)
        self.raise_for_status(response)

        return response

    def update_branch(self, git_sha, branch_ref) -> requests.Response:
        url = f"{self.github_domain}/git/{branch_ref}"
        request_body = {"sha": git_sha, "force": True}
        response = requests.post(url, headers=self.github_headers, json=request_body)
        self.raise_for_status(response)

        return response

    def _get_branch_sha(self, branch_name) -> str:
        url = f"{self.github_domain}/git/ref/heads/{branch_name}"
        response = requests.get(url, headers=self.github_headers)
        self.raise_for_status(response)

        branch_sha = response.json()["object"]["sha"]
        return branch_sha

    def create_pull_request(
        self,
        pr_title: str,
        head_branch_name: str,
        base_branch_name: str,
        pr_body: str = "",
    ) -> str:
        url = f"{self.github_domain}/pulls"
        request_body = {
            "title": pr_title,
            "head": head_branch_name,
            "base": base_branch_name,
            "body": pr_body,
        }

        response = requests.post(url, headers=self.github_headers, json=request_body)
        self.raise_for_status(response)

        return response.json()["url"]

    def get_branches(self, protected: bool = True) -> List[dict]:
        params = {"protected": protected}
        url = f"{self.github_domain}/branches"
        response = requests.get(url, params, headers=self.github_headers)
        self.raise_for_status(response)

        return response.json()

    def iterate_semantic_release_name(
        self,
        branches: List[dict],
        diff_magnitude: Literal["major", "minor", "patch"],
    ):
        version = {"major": 0, "minor": 0, "patch": 0}
        for branch in branches:
            branch_name = branch["name"]
            if not branch_name.startswith("release"):
                continue

            pattern = r"release/(?P<major>\d+)-(?P<minor>\d+)-(?P<patch>\d+)"
            m = re.match(pattern, branch_name)

            if int(m["major"]) > int(version["major"]):
                version["major"] = int(m["major"])
                version["minor"] = int(m["minor"])
                version["patch"] = int(m["patch"])
                continue
            if int(m["minor"]) > int(version["minor"]):
                version["minor"] = int(m["minor"])
                version["patch"] = int(m["patch"])
                continue
            if int(m["patch"]) > int(version["patch"]):
                version["patch"] = int(m["patch"])
                continue

        if diff_magnitude == "major":
            version["major"] += 1
            version["minor"] = 0
            version["patch"] = 0
        if diff_magnitude == "minor":
            version["minor"] += 1
            version["patch"] = 0
        if diff_magnitude == "patch":
            version["patch"] += 1

        return f"release/{version['major']}-{version['minor']}-{version['patch']}"

    def get_extant_pull_request_head_refs(self):
        url = f"{self.github_domain}/pulls"
        response = requests.get(url, headers=self.github_headers)
        self.raise_for_status(response)

        head_refs = [pr["head"]["ref"] for pr in response.json()]

        return head_refs
