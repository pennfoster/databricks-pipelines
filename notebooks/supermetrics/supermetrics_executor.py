# Databricks notebook source

dbutils.widgets.dropdown("environment", "dev", ["dev", "prd"])
# COMMAND -----
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# COMMAND -----
env = dbutils.widgets.get("environment")
child_nb_path = "./supermetrics_loader"


# child_nb_path = "databricks-pipelines/notebooks/supermetrics/child_dag.py"
# child_nb_path = "/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/databricks-pipelines/notebooks/supermterics/child_dag.py"
# child_nb_path = "/Workspace/Repos/bhorn@pennfoster.edu/databricks-pipelines.ide/databricks-pipelines/notebooks/supermetrics.py"
# COMMAND -----
# COMMAND -----
class Notebook:
    def __init__(self, path, timeout, parameters=None, retries=0) -> None:
        self.path = path
        self.timeout = timeout
        self.parameters = parameters
        self.retries = retries
        self.job_id = None
        self.job_url = None


def submit_notebook(notebook):
    try:
        if notebook.parameters:
            return dbutils.notebook.run(
                notebook.path, notebook.timeout, notebook.parameters
            )
        else:
            return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception:
        print(f"FAILED: notebook {notebook.path} with params: {notebook.parameters}")
        if notebook.retries < 1:
            raise
        print(
            "Retrying notebook {} with params: {}".format(
                notebook.path, notebook.parameters
            )
        )
        notebook.retries -= 1
        submit_notebook(notebook)


def parallel_notebooks(notebooks, max_parallel_tasks=1):
    # If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once.
    # This code limits the number of parallel notebooks.
    a = []
    b = []
    with ThreadPoolExecutor(max_workers=max_parallel_tasks) as tp:
        # futures = [tp.submit(submit_notebook, notebook) for notebook in enumerate(notebooks)]
        result = [tp.submit(submit_notebook, notebook) for notebook in notebooks]
        # for future in as_completed(result):
        #     # retrieve the result
        #     print(future.result())
        return result


# COMMAND -----
# df = pd.read_csv("/dbfs/mnt/sadatarawdev001_control/supermetrics/urls_for_apis.csv")
df = pd.read_csv("/dbfs/mnt/sadatarawdev001_control/supermetrics/urls_for_apis.csv")
df = df[df["C001_SearchName"].str.startswith("-") == False]
df.reset_index(drop=True, inplace=True)
df = df.iloc[0:3, :]  # TODO: for testing. remove after testing is complete
df.reset_index(drop=False, inplace=True)
# COMMAND -----
tasks = []
for i in df.index:
    row = df.iloc[i, :]
    timestamp = datetime.now().strftime("%Y%m%d %H:%M:%S")
    parameters = {
        "env": env,
        "search_name": row["C001_SearchName"],
        "query_name": row["C001_QueryName"],
        "url": row["C001_URL"],
        "custom_query": "no",
        "job_name": f'{row["C001_QueryName"]}_{datetime.now().strftime("%Y%m%d-%H%M%S")}',
    }
    notebook = Notebook(path=child_nb_path, timeout=0, parameters=parameters, retries=1)
    tasks.append(notebook)
# COMMAND -----
#  task_list = spark.sql("select * FROM /dbfs/mnt/sadatarawdev001_control/supermetrics/urls_for_apis").toPandas().to_dict('records')
res = parallel_notebooks(tasks, 2)
result = [f.result(timeout=3600) for f in res]  # This is a blocking call.
print(result)
# COMMAND -----
jobs_df = spark.read.json("/mnt/sadatarawdev001_control/supermetrics/job_idstest1.json")
jobs_df = jobs_df.toPandas()
for task in tasks:
    job_id = jobs_df[jobs_df["job_name"] == task.parameters["job_name"]][
        "job_id"
    ].values[0]
    job_url = f"https://adb-6104815453986823.3.azuredatabricks.net/?o=6104815453986823#job/{job_id}/run/1"
    task.job_id = job_id
    task.job_url = job_url
#  a = spark.sql(f"select * from json.`/mnt/sadatarawdev001_control/supermetrics/job_idstest1.json` where job_name = 'OtherFacebookCampaignV2_20230418-215824'").collect()
