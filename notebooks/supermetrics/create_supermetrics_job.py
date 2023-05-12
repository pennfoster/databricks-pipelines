# Databricks notebook source
# Create with:
#    databricks jobs create --json-file PATH

import pandas as pd
import json
from datetime import datetime
from pathlib import Path

from data_sources.supermetrics.functions import get_url_dataframe, save_json
from shared.functions.azure_utilities import get_mount_paths

# COMMAND -----
NAME = "supermetrics_loads"
CLUSTER_ID = "0412-221025-lwdq2fc5"
BRONZE_NOTEBOOK_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/notebooks/supermetrics/supermetrics_load_to_bronze"
SILVER_NOTEBOOK_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/notebooks/supermetrics/supermetrics_load_to_silver"
EMAIL_NOTIFICATIONS = {
    "on_success": [],
    "on_failure": ["bhorn@pennfoster.edu"],
    "no_alert_for_skipped_runs": False,
}
paths = get_mount_paths("supermetrics")
CONTROL_DIR = paths.control

# COMMAND -----
df = get_url_dataframe()

bronze_tasks = []
for i in df.index:
    row = df.iloc[i, :]
    # timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S")
    parameters = {
        "query_name": row["C001_QueryName"],
    }

    task = {
        "task_key": f'{row["C001_QueryName"]}_bronze',
        "notebook_task": {
            "notebook_path": BRONZE_NOTEBOOK_PATH,
            "base_parameters": parameters,
        },
        "existing_cluster_id": CLUSTER_ID,
    }
    bronze_tasks.append(task)
# COMMAND -----
silver_tasks = []
for search_name in df["C001_SearchName"].unique():
    parameters = {"search_name": search_name}
    dependents = [
        {"task_key": key}
        for key in df[df["C001_SearchName"] == search_name]["C001_QueryName"].tolist()
    ]

    task = {
        "task_key": f"{search_name}_silver",
        "notebook_task": {
            "notebook_path": SILVER_NOTEBOOK_PATH,
            "base_parameters": parameters,
        },
        "depends_on": dependents,
        "existing_cluster_id": CLUSTER_ID,
    }
    silver_tasks.append(task)
# COMMAND -----
tasks = bronze_tasks + silver_tasks
# COMMAND -----
config = {
    "settings": {},
    "name": NAME,
    "email_notifications": EMAIL_NOTIFICATIONS,
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": tasks,
    "format": "MULTI_TASK",
}


json_path = save_json(
    dest_dir=f"/dbfs/{CONTROL_DIR}",
    file_name=f"create_supermetrics_job",
    data=config,
)
