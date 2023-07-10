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
DATASOURCE = "googlenonsearchads"
# "bingadgroup"
# "bingcampaign"
# "bingdevice"
# "bingkeyword"
# "bingsearchads"
# "facebookads"
# "facebookadset"
# "facebookcampaign"
# "googleadgroup"
# "googlecampaign"
# "googlekeyword"
# "googlenonsearchads"
# "googlesearchads"

NAME = f"supermetrics_{DATASOURCE}_loads_raw_bronze_silver"

# CLUSTER_ID = "0412-221025-lwdq2fc5"
REPO_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide"
REQUIREMENTS_PATH = f"{REPO_PATH}/notebooks/supermetrics/requirements.sh"
BRONZE_NOTEBOOK_PATH = (
    f"{REPO_PATH}/notebooks/supermetrics/bronze/supermetrics_load_to_bronze"
)
# SILVER_NOTEBOOK_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/notebooks/supermetrics/supermetrics_load_to_silver"
EMAIL_NOTIFICATIONS = {
    "on_success": [],
    "on_failure": ["bhorn@pennfoster.edu"],
    "no_alert_for_skipped_runs": False,
}
JOB_CLUSTER_KEY = "supermetrics_loads_raw_bronze_silver"
JOB_CLUSTER = {
    "job_cluster_key": f"{JOB_CLUSTER_KEY}",
    "new_cluster": {
        "cluster_name": "",
        "spark_version": "12.2.x-scala2.12",
        "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "ON_DEMAND_AZURE",
            "spot_bid_max_price": -1,
        },
        "node_type_id": "Standard_D3_v2",
        "enable_elastic_disk": True,
        "init_scripts": [
            {
                "workspace": {
                    "destination": f"{REPO_PATH}/notebooks/supermetrics/requirements.sh"
                }
            }
        ],
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 2,
    },
}

paths = get_mount_paths("supermetrics")
CONTROL_DIR = paths.control
# COMMAND -----
df = get_url_dataframe()
df = df[df["C001_SearchName"].str.lower() == DATASOURCE.lower()]
df.reset_index(drop=True, inplace=True)

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
        # "existing_cluster_id": CLUSTER_ID,
        "job_cluster_key": JOB_CLUSTER_KEY,
        "max_retries": 2,
        "min_retry_interval_millis": 300000,
        "retry_on_timeout": True,
        "timeout_seconds": 1200,
    }
    bronze_tasks.append(task)
# COMMAND -----
silver_tasks = []
for search_name in df["C001_SearchName"].unique():
    silver_notebook_path = f"{REPO_PATH}/notebooks/supermetrics/silver/supermetrics_{search_name.lower()}_silver"
    parameters = {"search_name": search_name}
    dependents = [
        {"task_key": f"{key}_bronze"}
        for key in df[df["C001_SearchName"] == search_name]["C001_QueryName"].tolist()
    ]

    task = {
        "task_key": f"{search_name}_silver",
        "notebook_task": {
            "notebook_path": silver_notebook_path,
            "base_parameters": parameters,
        },
        "depends_on": dependents,
        "job_cluster_key": JOB_CLUSTER_KEY,
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
    "job_clusters": [JOB_CLUSTER],
    "format": "MULTI_TASK",
}


json_path = save_json(
    dest_dir=f"/dbfs/{CONTROL_DIR}",
    file_name=f"supermetrics_{DATASOURCE.lower()}_job",
    data=config,
)
