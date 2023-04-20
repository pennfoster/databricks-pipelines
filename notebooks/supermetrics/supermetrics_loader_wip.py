# Databricks notebook source

# COMMAND -----
import json
import pandas as pd
import re
import requests
from pathlib import Path
from datetime import datetime


# COMMAND -----
# def save_json(dest_path, file_name, data, suffix=None):
#     Path(dest_path).mkdir(parents=True, exist_ok=True)
#     file_name = file_name.replace(".json", "")
#     if suffix == "timestamp":
#         file_name += f'_{datetime.now().strftime("%Y%m%d-%H%M%S")}'
#     # if suffix="uuid":
#     #     pass
#     file_path = f"{dest_path}/{file_name}.json"
#     with open(f"{dest_path}/{file_name}.json", "w") as file:
#         file.write(json.dumps(data))
#     return file_path


# def load_json(file_path):
#     with open(file_path, "r") as file:
#         data = json.load(file)
#     return data


# def pass_job_id():
#     notebook_info = json.loads(
#         dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
#     )
#     try:
#         job_id = notebook_info["tags"]["jobId"]
#     except:
#         job_id = -1
#     dbutils.notebook.exit(job_id)


# def get_url_dataframe(env):
#     df = pd.read_csv(
#         f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_api_rev.csv"
#     )
#     df = df[df["C001_SearchName"].str.startswith("-") == False]
#     df.reset_index(drop=True, inplace=True)
#     return df


# class Supermetrics:
#     def __init__(self) -> None:
#         self.short_domain = ""

#     def _set_response_structure(self, json_response):
#         resp_keys = list(json_response.keys())
#         resp_keys.sort()

#         if "error" in resp_keys:
#             raise RuntimeError(
#                 f"{json_response['error']['message']}: {json_response['error']['description']}"
#             )
#         if resp_keys == ["columns", "data", "fields", "notes"]:
#             self.resp_structure = "columns,data,fields,notes"
#             self.fields = json_response["fields"]
#             self.status = json_response["notes"]["status"].lower()
#         elif resp_keys == ["data", "meta"]:
#             self.resp_structure = "data,meta"
#             self.fields = json_response["meta"]["query"]["fields"]
#             self.status = json_response["meta"]["status_code"].lower()
#         else:
#             raise TypeError(
#                 "Response does not match known format. Check URL is correct or map new json response structure."
#             )
#         self.data = json_response["data"]

#     def get_data(self, url, params=None):
#         if params:
#             url = f"{self.short_url}/{url}"
#         try:
#             response = requests.get(url=url, params=params)
#         except requests.exceptions.RequestException as e:
#             raise e

#         self._set_response_structure(response.json())

#         return response.json()


# COMMAND -----
dbutils.widgets.dropdown("environment", "dev", ["dev", "prd"])
dbutils.widgets.dropdown("custom_query", "no", ["no", "yes"])
env = dbutils.widgets.get("environment")
custom_query = True if dbutils.widgets.get("custom_query") == "yes" else False
if custom_query:
    dbutils.widgets.text("start_date", "")
    dbutils.widgets.text("end_date", "")
    url_df = get_url_dataframe(env)
    queries = url_df["C001_QueryName"].sort_values().to_list()
    queries += [""]
    dbutils.widgets.dropdown("query_name", "", queries)
else:
    dbutils.widgets.text("query_name", "*NA*")
    dbutils.widgets.text("search_name", "*NA*")
    dbutils.widgets.text("url", "*NA*")
# else:
#     dbutils.widgets.remove("start_date")
#     dbutils.widgets.remove("end_date")
#     dbutils.widgets.remove("custom_column_list")
# else:
#     dbutils.widgets.remove("start_date")
#     dbutils.widgets.remove("end_date")
#     dbutils.widgets.remove("custom_column_list")
# COMMAND -----
# configs
query_name = dbutils.widgets.get("query_name")
row = url_df[url_df["C001_QueryName"] == query_name]
url = row["C001_URL"].values[0]
search_name = row["C001_SearchName"].values[0]
# job_name = dbutils.widgets.get("job_name")
if custom_query:
    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")
    date_range = (
        None
        if start_date == ""
        else f'"start_date":"{start_date}","end_date":"{end_date}",'
    )
    if start_date == "":
        url = re.sub('(?<=)"date_range.+?(?<=\,)', date_range, url)

env = "dev"
raw_path = f"mnt/sadataraw{env}001/supermetrics/{search_name}/{query_name}"
bronze_path = f"mnt/bronze/supermetrics/{search_name}/{query_name}"


# COMMAND -----
sm = Supermetrics()
resp_json = sm.get_data(url)

# COMMAND -----
# Save to raw
json_path = save_json(
    dest_path=f"/dbfs/{raw_path}",
    file_name=f"{search_name}_{query_name}",
    data=resp_json,
    suffix="timestamp",
)
# COMMAND -----
# Load from raw and save to bronze
bronze_path = f"mnt/bronze/supermetrics/{search_name}/{query_name}"
sm_json = load_json(json_path)
cols = [i["field_id"].lower() for i in sm_json["meta"]["query"]["fields"]]
data = sm_json["data"][1:]
df = pd.DataFrame(columns=cols, data=data)
df["raw_file_name"] = json_path.split("/")[-1]
df = df.applymap(str)

# sparkdf = spark.createDataFrame(df)
# # COMMAND -----
sparkdf.write.format("delta").mode("append").save(bronze_path + "testing.delta")
# COMMAND -----
dbutils.notebook.exit("SUCCESS")
