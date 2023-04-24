# Databricks notebook source
# DBTITLE 1, Setup
from orchestration.functions import get_current_env_jobs, get_job_settings_from_github
from shared.functions.github_utilities import get_pr_files
from shared.functions.azure_utilities import get_key_vault_scope
import requests, json

jobs_repo = "databricks-pipelines"
jobs_branch = "cicd/test_jobs_in_pr"
jobs_path = "orchestration/dbricks_jobs"
pull_request_number = 6  # TODO PR param from request

# COMMAND ----------
# DBTITLE 1, Get Extant and New Jobs Metadata
pr_files = get_pr_files(jobs_repo, pull_request_number, jobs_path)
extant_jobs = get_current_env_jobs()

jobs_to_process = []
for fp, status in pr_files:
    pr_job = get_job_settings_from_github(jobs_repo, fp, jobs_branch)
    extant_id = None
    for e in extant_jobs:
        if e.name.lower() == pr_job.name.lower():
            pr_job = pr_job._replace(id=e.id)
            break

    jobs_to_process.append((pr_job, status))

# COMMAND ----------
# DBTITLE 1, Destroy, Update, & Create PR Jobs
output_job_ids = []

session = requests.Session()
url = f"https://{sc.getConf().get('spark.databricks.workspaceUrl')}/api/2.1/jobs"
session.headers = {
    **session.headers,
    "Authorization": f"Bearer {dbutils.secrets.get(get_key_vault_scope(), 'cicd-access-token')}",
}
for job, status in jobs_to_process:
    job.settings["schedule"]["pause_status"] = "PAUSED"
    # 'added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged'
    if status == "removed":
        request_body = {"job_id": job.id}
        response = session.post(f"{url}/delete", json=request_body)
        response.raise_for_status()
        print(f"Job {job.id} ({job.name}) deleted.")
    elif job.id:
        request_body = {"job_id": job.id, "new_settings": job.settings}
        response = session.post(f"{url}/update", json={"job_id": job.id})
        response.raise_for_status()
        print(f"Job {job.id} ({job.name}) updated.")
        output_job_ids.append(job.id)
    elif not job.id:
        request_body = job.settings
        response = session.post(f"{url}/create", json=request_body)
        response.raise_for_status()
        print(f"Job {response.json()['job_id']} ({job.name}) created.")
        output_job_ids.append(job.id)

# COMMAND ----------
# DBTITLE 1, Output Updated Job IDs JSON String

dbutils.notebook.exit(json.dumps(output_job_ids))
