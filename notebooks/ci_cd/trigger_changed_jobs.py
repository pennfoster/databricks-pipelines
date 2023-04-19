# Databricks notebook source
# DBTITLE 1, Setup
import requests
import aiohttp
import logging
from typing import NamedTuple

from typing import List, Tuple, Union
from shared.functions.azure_utilities import get_key_vault_scope
from shared.functions.github_utilities import get_pr_files

workspace_url = sc.getConf().get("spark.databricks.workspaceUrl")
jobs_repo = "databricks-pipelines"
jobs_path = "orchestration/dbricks_jobs"
pull_request_number = 5


class Job(NamedTuple):
    id: int
    name: str
    settings: dict


# Job = namedtuple("Job", )
# COMMAND ----------


# COMMAND ----------
def get_current_env_jobs(workspace_url: str):
    data = []
    with requests.Session() as s:
        s.headers = {
            "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}"
        }
        s.params = {"limit": 25, "offset": 0, "expand_tasks": "true"}
        while True:
            response = s.get(f"https://{workspace_url}/api/2.1/jobs/list")
            data.extend(response.json()["jobs"])
            if not response.json()["has_more"]:
                break
            s.params["offset"] = len(data)

    extant_jobs = [
        Job(
            id=j["job_id"],
            name=j["settings"]["name"],
            settings=j["settings"],
        )
        for j in data
    ]

    unique_job_names = set([j.settings["name"].lower() for j in extant_jobs])
    if len(unique_job_names) != len(extant_jobs):
        job_count = {}
        for name in unique_job_names:
            job_count[name] = 0
        for job in extant_jobs:
            job_count[job.settings["name"].lower()] += 1
        for k, v in job_count.items():
            if v > 1:
                logging.error("%s jobs named %s" % (v, k))
        raise ValueError(
            "Two or more DBricks jobs have non-unique names, which will break CI/CD flow."
        )

    return extant_jobs


def get_job_settings_from_repo(github_filepath: str):
    url = f"https://api.github.com/repos/pennfoster/{jobs_repo}/contents/{github_filepath}"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    job_data = response.json()

    return Job(
        id=None,
        name=job_data["settings"]["name"],
        settings=job_data["settings"],
    )


#  TODO open the damn json file...

# COMMAND ----------
pr_files = get_pr_files(jobs_repo, pull_request_number, jobs_path)
extant_jobs = get_current_env_jobs(workspace_url)
jobs_to_process = []

print(pr_files)
# print()
for fp, status in pr_files:
    pr_job = get_job_settings_from_repo(fp)
    extant_id = None
    for e in extant_jobs:
        if e.name.lower() == pr_job.name.lower():
            pr_job.id = e.id
            break

    jobs_to_process.append((pr_job, status))

print(jobs_to_process)
# COMMAND ----------


async def update_job_coro(async_session: aiohttp.ClientSession, job_settings: str):
    pass


async def create_job_coro(async_session: aiohttp.ClientSession, job_filepath: str):
    create_job_url = f"https://{workspace_url}/api/2.1/jobs/create"


async def remove_job_coro(async_session: aiohttp.ClientSession, job_filepath: str):
    pass
