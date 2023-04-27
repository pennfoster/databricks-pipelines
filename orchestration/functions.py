import json, logging, requests
from typing import NamedTuple, List
from hashlib import sha256

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


class Job(NamedTuple):
    id: int
    name: str
    settings: dict
    hash: str


def get_repo_jobs(repo: str, path_to_jobs: str, branch: str = "master") -> List[Job]:
    url = f"https://api.github.com/repos/pennfoster/{repo}/contents/{path_to_jobs}"

    with requests.Session() as s:
        s.headers = {
            **s.headers,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        jobs_dir = s.get(url, params={"ref": branch})
        jobs_dir.raise_for_status()

        job_data = [
            json.loads(s.get(job["download_url"]).text) for job in jobs_dir.json()
        ]

    assert len(set([j["name"].lower() for j in job_data])) == len(job_data)

    return [
        Job(
            id=None,
            name=job["name"],
            settings=job,
            hash=sha256(json.dumps(job, sort_keys=True).encode("utf-8")).hexdigest(),
        )
        for job in job_data
    ]

def get_current_env_jobs() -> List[Job]:
    env_domain = sc.getConf().get("spark.databricks.workspaceUrl")
    data = []
    with requests.Session() as s:
        s.headers = {
            **s.headers,
            "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}",
        }
        s.params = {"limit": 25, "offset": 0, "expand_tasks": "true"}
        while True:
            response = s.get(f"https://{env_domain}/api/2.1/jobs/list")
            data.extend(response.json()["jobs"])
            if not response.json()["has_more"]:
                break
            s.params["offset"] = len(data)

    extant_jobs: List[Job] = []
    for job in data:
        if job["settings"].get("tags", {}).get("workflow") != "CI/CD":
            continue
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

    unique_job_names = set([j.name.lower() for j in extant_jobs])
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
            "Two or more DBricks CI/CD jobs have non-unique names, which will break CI/CD flow."
        )

    return extant_jobs


# def get_job_settings_from_github(repo, filepath: str, branch: str = "master"):
#     url = f"https://api.github.com/repos/pennfoster/{repo}/contents/{filepath}"
#     headers = {
#         "Accept": "application/vnd.github+json",
#         "X-GitHub-Api-Version": "2022-11-28",
#     }
#     response = requests.get(url, headers=headers, params={"ref": branch})
#     response.raise_for_status()

#     job_data = json.loads(requests.get(response.json()["download_url"]).text)

#     return Job(
#         id=None,
#         name=job_data["settings"]["name"],
#         settings=job_data["settings"],
#     )
