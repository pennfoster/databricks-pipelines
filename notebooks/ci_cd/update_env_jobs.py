# Databricks notebook source
# DBTITLE 1, Setup
import json
import requests

from orchestration.functions import get_current_env_jobs, get_repo_jobs
from shared.functions.azure_utilities import get_key_vault_scope

jobs_repo = "databricks-pipelines"
jobs_path = "orchestration/dbricks_jobs"
jobs_branch = "cicd/test_jobs_in_pr"  # TODO remove

# COMMAND ----------
# DBTITLE 1, Get Extant and New Jobs Metadataz


repo_jobs = get_repo_jobs(jobs_repo, jobs_path, jobs_branch)
extant_jobs = get_current_env_jobs()

extant_job_names = set([e.name for e in extant_jobs])
repo_job_names = set([r.name for r in repo_jobs])

for job in repo_jobs:
    for e in extant_jobs:
        if e.name == job.name:
            job = job._replace(id=e.id)

to_be_added = repo_job_names - extant_job_names
to_be_updated = extant_job_names & repo_job_names
to_be_deleted = extant_job_names - repo_job_names

# tests whether there are any common jobs in the lists
assert set(to_be_added).intersection(*[to_be_updated, to_be_deleted]) == set()

# COMMAND ----------
# DBTITLE 1, Destroy, Update, & Create PR Jobs
output_job_ids = []

session = requests.Session()
url = f"https://{sc.getConf().get('spark.databricks.workspaceUrl')}/api/2.1/jobs"
session.headers = {
    **session.headers,
    "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}",
}
for job in extant_jobs:
    if job.name in to_be_deleted:
        request_body = {"job_id": e.id}
        response = session.post(f"{url}/delete", json=request_body)
        response.raise_for_status()
        print(f"Job {e.id} ({e.name}) deleted.")

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
