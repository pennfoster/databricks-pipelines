# Databricks notebook source
# DBTITLE 1, Setup
import requests
from shared.functions.azure_utilities import get_key_vault_scope

repo_jobs = []
extant_jobs = []
new_jobs = []
updated_jobs = []
workspace_url = sc.getConf().get("spark.databricks.workspaceUrl")
jobs_repo = "databricks-pipelines"
jobs_path = "orchestration/dbricks_jobs"

# COMMAND ----------
# DBTITLE 1, Get Github Job Records
with requests.Session() as s:
    github_auth = None
    s.headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    jobs_directory = s.get(
        f"https://api.github.com/repos/pennfoster/{jobs_repo}/contents/{jobs_path}",
    )
    for job in jobs_directory.json():
        job_body = s.get(job["download_url"])
        repo_jobs.append(job_body.json()["settings"])

# COMMAND ----------
# DBTITLE 1, Get current environment extant jobs:
dbricks_session = requests.Session()
dbricks_session.headers = {
    "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}"
}

params = {"limit": 25, "offset": 0, "expand_tasks": "true"}
while True:
    job_list_url = f"https://{workspace_url}/api/2.1/jobs/list"
    response = dbricks_session.get(job_list_url, params=params)
    extant_jobs.extend(response.json()["jobs"])
    if not response.json()["has_more"]:
        break
    params["offset"] = len(extant_jobs)

# COMMAND ----------
# DBTITLE 1, Post new jobs where job name not in extant jobs

create_job_url = f"https://{workspace_url}/api/2.1/jobs/create"
extant_job_names = [j["settings"]["name"] for j in extant_jobs]

for job in repo_jobs:
    if job_name in extant_job_names:
        if job["settings"]:
            pass

    job_name = job["name"]
    if job_name not in extant_job_names:
        response = dbricks_session.post(create_job_url, json=job)
        print("job created:")
        print(response.json())


# COMMAND ----------
# DBTITLE 1, Teardown
dbricks_session.close()
