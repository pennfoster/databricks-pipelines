import pandas as pd
import json
from datetime import datetime
from pathlib import Path

from data_sources.supermetrics.functions import get_url_dataframe, save_json

NAME = "supermetrics_loads"
ENV = "dev"
CLUSTER_ID = "0412-221025-lwdq2fc5"
NOTEBOOK_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/notebooks/supermetrics/supermetrics_load_to_bronze"
EMAIL_NOTIFICATIONS = {
    "on_success": [],
    "on_failure": ["bhorn@pennfoster.edu"],
    "no_alert_for_skipped_runs": False,
}
RAW_LANDING_DIR = f"mnt/sadataraw{ENV}001_control/supermetrics"

df = get_url_dataframe(ENV)

# Create tasks
tasks = []
for i in df.index:
    row = df.iloc[i, :]
    timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S")
    parameters = {
        "env": ENV,
        "query_name": row["C001_QueryName"],
    }

    task = {
        "task_key": row["C001_QueryName"],
        "notebook_task": {
            "notebook_path": NOTEBOOK_PATH,
            "base_parameters": parameters,
        },
        "existing_cluster_id": CLUSTER_ID,
    }
    tasks.append(task)

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
    dest_dir=f"/dbfs/{RAW_LANDING_DIR}",
    file_name=f"create_supermetrics_job",
    data=config,
)
