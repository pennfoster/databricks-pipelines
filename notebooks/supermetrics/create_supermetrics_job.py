import pandas as pd
import json
from datetime import datetime
from pathlib import Path

NAME = "supermetrics_loads"
ENV = "dev"
CLUSTER_ID = "0513-185437-93k1ikbc"
NOTEBOOK_PATH = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/notebooks/supermetrics/supermetrics_loader_wip"
EMAIL_NOTIFICATIONS = {
    "on_success": [],
    "on_failure": ["bhorn@pennfoster.edu"],
    "no_alert_for_skipped_runs": False,
}
RAW_LANDING_DIR = f"mnt/sadataraw{ENV}001_control/supermetrics"


def get_url_dataframe(env):
    df = pd.read_csv(
        f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_apis.csv"
    )
    df = df[df["C001_SearchName"].str.startswith("-") == False]
    df.reset_index(drop=True, inplace=True)
    return df


def save_json(dest_path, file_name, data, suffix=None):
    Path(dest_path).mkdir(parents=True, exist_ok=True)
    file_name = file_name.replace(".json", "")
    if suffix == "timestamp":
        file_name += f'_{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    # if suffix="uuid":
    #     pass
    file_path = f"{dest_path}/{file_name}.json"
    with open(f"{dest_path}/{file_name}.json", "w") as file:
        file.write(json.dumps(data))
    return file_path


df = get_url_dataframe(ENV)
df = df.iloc[0:10, :]

# Create tasks
tasks = []
for i in df.index:
    row = df.iloc[i, :]
    timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S")
    parameters = {
        "env": ENV,
        "search_name": row["C001_SearchName"],
        "query_name": row["C001_QueryName"],
        "url": row["C001_URL"],
        "custom_query": "no",
        "job_name": f'{row["C001_QueryName"]}_{datetime.now().strftime("%Y%m%d-%H%M%S")}',
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

# Create settings
# settings = {
#     "name": "supermetrics_loads_test",
#     "email_notifications": EMAIL_NOTIFICATIONS,
# }

# settings = {
#     "name": "supermetrics_loads_test",
#     "email_notifications": EMAIL_NOTIFICATIONS,
#     "webhook_notifications": {},
#     "timeout_seconds": 0,
#     "max_concurrent_runs": 1,
#     "tasks": tasks,
#     "format": "MULTI_TASK",
# }


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

# config = {
#     "settings": settings,
# }


json_path = save_json(
    dest_path=f"/dbfs/{RAW_LANDING_DIR}",
    file_name=f"create_supermetrics_job",
    data=config,
)
