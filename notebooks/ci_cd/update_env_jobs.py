# Databricks notebook source
# DBTITLE 1, Setup
import json
import requests

from orchestration.functions import get_current_env_jobs, get_repo_jobs
from shared.functions.azure_utilities import get_key_vault_scope

jobs_repo = "databricks-pipelines"
jobs_path = "orchestration/dbricks_jobs"
jobs_branch = "staging"  # TODO remove

# COMMAND ----------
# DBTITLE 1, Get Extant and New Jobs Metadataz


repo_jobs = get_repo_jobs(jobs_repo, jobs_path, jobs_branch)
extant_jobs = get_current_env_jobs()

to_be_added = []
to_be_updated = []
to_be_removed = []

extant_job_hashes = [j.hash for j in extant_jobs]
extant_job_names = [j.name for j in extant_jobs]
for repj in repo_jobs:
    if repj.hash in extant_job_hashes:
        continue
    if not repj.name in extant_job_names:
        to_be_added.append(repj)
        continue
    for extj in extant_jobs:
        if repj.name in extant_job_names:
            job = repj._replace(id=extj.id)
            to_be_updated.append(job)

repo_job_names = [j.name for j in repo_jobs]
for job in extant_jobs:
    if not job.name in repo_job_names:
        to_be_removed.append(job)

# COMMAND ----------
# DBTITLE 1, Destroy, Update, & Create PR Jobs
output_job_ids = []

session = requests.Session()
url = f"https://{sc.getConf().get('spark.databricks.workspaceUrl')}/api/2.1/jobs"
session.headers = {
    **session.headers,
    "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}",
}
for job in to_be_removed:
    request_body = {"job_id": job.id}
    response = session.post(f"{url}/delete", json=request_body)
    response.raise_for_status()
    print(f"Job {job.id} ({job.name}) deleted.")

for job in repo_jobs:
    if job.name in to_be_updated:
        request_body = {"job_id": job.id, "new_settings": job.settings}
        response = session.post(f"{url}/update", json={"job_id": job.id})
        response.raise_for_status()
        print(f"Job {job.id} ({job.name}) updated.")
        output_job_ids.append(job.id)

    if job.name in to_be_added:
        request_body = job.settings
        response = session.post(f"{url}/create", json=request_body)
        response.raise_for_status()
        print(f"Job {response.json()['job_id']} ({job.name}) created.")
        output_job_ids.append(job.id)


# COMMAND ----------
# DBTITLE 1, Output Updated Job IDs JSON String

session.close()
dbutils.notebook.exit(json.dumps(output_job_ids))
