# Databricks notebook source
# DBTITLE 1, Setup
import json
from typing import List

from shared.classes import Orchestration, Job

jobs_repo = "databricks-pipelines"
jobs_path = "orchestration/dbricks_jobs"

dbutils.widgets.text("jobs_branch", defaultValue="staging")
jobs_branch = dbutils.widgets.get("jobs_branch")

# COMMAND ----------
# DBTITLE 1, Get Extant and New Jobs Metadataz

orch = Orchestration()
repo_jobs = orch.get_repo_jobs(jobs_repo, jobs_path, jobs_branch)
extant_jobs = orch.get_current_env_jobs()

to_be_added: List[Job] = []
to_be_updated: List[Job] = []
to_be_removed: List[Job] = []

extant_job_hashes = [j.hash for j in extant_jobs]
extant_job_names = [j.name for j in extant_jobs]
for repj in repo_jobs:
    if repj.hash in extant_job_hashes:
        continue
    if not repj.name in extant_job_names:
        to_be_added.append(repj)
        continue
    for extj in extant_jobs:
        if repj.name == extj.name:
            job = repj._replace(id=extj.id)
            to_be_updated.append(job)

repo_job_names = [j.name for j in repo_jobs]
for job in extant_jobs:
    if not job.name in repo_job_names:
        to_be_removed.append(job)

# COMMAND ----------
# DBTITLE 1, Destroy, Update, & Create PR Jobs
output_job_ids = []

for job in to_be_removed:
    orch.delete_job(job.id)
    print(f"Job {job.id} ({job.name}) deleted.")

for job in to_be_updated:
    orch.update_job(job.id, job.settings)
    output_job_ids.append(job.id)
    print(f"Job {job.id} ({job.name}) updated.")

for job in to_be_added:
    new_job_id = orch.create_job(job.settings)
    output_job_ids.append(new_job_id)
    print(f"Job {new_job_id} ({job.name}) created.")


# COMMAND ----------
# DBTITLE 1, Output Updated Job IDs JSON String

dbutils.notebook.exit(json.dumps(output_job_ids))
