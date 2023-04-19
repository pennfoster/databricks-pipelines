import json, logging, requests
from typing import NamedTuple

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


def get_job_settings_from_github(repo, filepath: str, branch: str = "master"):
    url = f"https://api.github.com/repos/pennfoster/{repo}/contents/{filepath}"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.get(url, headers=headers, params={"ref": branch})
    response.raise_for_status()

    job_data = json.loads(requests.get(response.json()["download_url"]).text)

    return Job(
        id=None,
        name=job_data["settings"]["name"],
        settings=job_data["settings"],
    )


def get_current_env_jobs():
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
        # raise ValueError(
        #     "Two or more DBricks jobs have non-unique names, which will break CI/CD flow."
        # )

    return extant_jobs
