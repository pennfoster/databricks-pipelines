# Databricks notebook source

# COMMAND -----
dbutils.widgets.dropdown("environment", "dev", ["dev", "prd"])
dbutils.widgets.text("url", "")
dbutils.widgets.text("search_name", "")
dbutils.widgets.text("query_name", "")
dbutils.widgets.text("job_name", "")
dbutils.widgets.dropdown("custom_query", "no", ["no", "yes"])
custom_query = True if dbutils.widgets.get("custom_query") == "yes" else False
if custom_query:
    dbutils.widgets.text("start_date", "")
    dbutils.widgets.text("end_date", "")
    dbutils.widgets.text("custom_column_list", "")
# else:
#     dbutils.widgets.remove("start_date")
#     dbutils.widgets.remove("end_date")
#     dbutils.widgets.remove("custom_column_list")
# COMMAND -----
import json
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime

# COMMAND -----
# configs
url = dbutils.widgets.get("url")
search_name = dbutils.widgets.get("search_name")
query_name = dbutils.widgets.get("query_name")
job_name = dbutils.widgets.get("job_name")

# search_name = "test_search2"
# query_name = "test_query2"
env = "dev"
raw_path = f"mnt/sadataraw{env}001/supermetrics/{search_name}/{query_name}"
bronze_path = f"mnt/bronze/supermetrics/{search_name}/{query_name}"

notebook_info = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
try:
    job_id = notebook_info["tags"]["jobId"]
except:
    job_id = -1
# job_name = f"{query_name.lower()}_{datetime.now().strftime("%Y%m%d-%H%M%S")}"

job_df = pd.DataFrame(
    columns=["job_name", "job_id", "insert_date"],
    data=[[job_name, job_id, datetime.now().strftime("%Y%m%d-%H:%M:%S")]],
)
spjob_df = spark.createDataFrame(job_df)
# spjob_df.coalesce(1).write.mode('append').option("header","true").csv(f'/mnt/sadataraw{env}001_control/supermetrics/job_idstest5.csv')
spjob_df.write.mode("append").json(
    f"/mnt/sadatarawdev001_control/supermetrics/job_idstest1.json"
)

# COMMAND -----
# url = "https://api.supermetrics.com/enterprise/v2/query/data/json?json=%7B%22ds_id%22%3A%22AW%22%2C%22ds_accounts%22%3A%5B%228180710541%22%5D%2C%22ds_user%22%3A%22supermetricsapi%40gmail.com%22%2C%22date_range_type%22%3A%22last_7_days%22%2C%22fields%22%3A%5B%22Date%22%2C%22AdID%22%2C%22CustomUrlParameters%22%2C%22profile%22%2C%22profileID%22%2C%22Network%22%2C%22Campaignname%22%2C%22CampaignID%22%2C%22Campaignstatus%22%2C%22Adgroupname%22%2C%22AdgroupID%22%2C%22Adgroupstatus%22%2C%22Headline%22%2C%22HeadlinePart1%22%2C%22HeadlinePart2%22%2C%22HeadlinePart3%22%2C%22path1%22%2C%22path2%22%2C%22Description%22%2C%22Description1%22%2C%22Description2%22%2C%22TextAdDescription1%22%2C%22TextAdDescription2%22%2C%22Adtype%22%2C%22Adstatus%22%2C%22finalURL%22%2C%22TrackingUrlTemplate%22%2C%22campaignlabels%22%2C%22adgrouplabels%22%2C%22adlabels%22%2C%22Labels%22%2C%22Impressions%22%2C%22Clicks%22%2C%22Cost%22%2C%22Ctr%22%2C%22CPC%22%2C%22CPM%22%2C%22Conversions%22%2C%22ConversionRate%22%2C%22CPI%22%2C%22CostPerConversion%22%2C%22ValuePerConvManyPerClick%22%2C%22ConversionValue%22%2C%22ROAS%22%2C%22Viewthroughconversions%22%2C%22EstimatedTotalConversions%22%2C%22EstimatedTotalConversionValue%22%5D%2C%22filter%22%3A%22Network+%3D%40+search%22%2C%22max_rows%22%3A1000000%2C%22api_key%22%3A%22api_IwLe9xbnN3DgFQF9mXq8UxyavrGeO0FZYjI_V88ff6lqqid1xed8k89oybxD7FxzJTuR_Ty8PkAwJ6t811lK2XkefTo5OpFlR9Sw%22%7D"
# query_name = "testing"
# search_name = "testing"


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


def load_json(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def pass_job_id():
    notebook_info = json.loads(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    )
    try:
        job_id = notebook_info["tags"]["jobId"]
    except:
        job_id = -1
    dbutils.notebook.exit(job_id)


def get_url_dataframe(env):
    df = pd.read_csv(
        f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_apis.csv"
    )
    df = df[df["C001_SearchName"].str.startswith("-") == False]
    df.reset_index(drop=True, inplace=True)
    return df


class Supermetrics:
    def __init__(self) -> None:
        self.short_domain = ""

    def _set_response_structure(self, json_response):
        resp_keys = list(json_response.keys())
        resp_keys.sort()

        if "error" in resp_keys:
            raise RuntimeError(
                f"{json_response['error']['message']}: {json_response['error']['description']}"
            )
        if resp_keys == ["columns", "data", "fields", "notes"]:
            self.resp_structure = "columns,data,fields,notes"
            self.fields = json_response["fields"]
            self.status = json_response["notes"]["status"].lower()
        elif resp_keys == ["data", "meta"]:
            self.resp_structure = "data,meta"
            self.fields = json_response["meta"]["query"]["fields"]
            self.status = json_response["meta"]["status_code"].lower()
        else:
            raise TypeError(
                "Response does not match known format. Check URL is correct or map new json response structure."
            )
        self.data = json_response["data"]

    def get_data(self, url, params=None):
        if params:
            url = f"{self.short_url}/{url}"
        try:
            response = requests.get(url=url, params=params)
        except requests.exceptions.RequestException as e:
            raise e

        self._set_response_structure(response.json())

        return response.json()


# COMMAND -----

# dbutils.notebook.exit(job_id)
# pass_job_id()
# %scala val runId = dbutils.notebook.getContext .currentRunId .getOrElse(System.currentTimeMillis() / 1000L) .toString
# Seq(runId).toDF("run_id").createOrReplaceTempView("run_id")
# COMMAND -----
# runId = spark.table("run_id").head()["run_id"]
# COMMAND -----
# get data
sm = Supermetrics()
resp_json = sm.get_data(url)

# COMMAND -----
# Save to raw
# json_path = save_json(
#     dest_path=f"/dbfs/{raw_path}",
#     file_name=f"{search_name}_{query_name}",
#     data=resp_json,
#     suffix="timestamp",
# )
# # COMMAND -----
# # Load from raw and save to bronze
# bronze_path = f"mnt/bronze/supermetrics/{search_name}/{query_name}"
# sm_json = load_json(json_path)
# cols = [i["field_id"].lower() for i in sm_json["meta"]["query"]["fields"]]
# data = sm_json["data"][1:]
# df = pd.DataFrame(columns=cols, data=data)
# df["raw_file_name"] = json_path.split("/")[-1]
# df = df.applymap(str)

# sparkdf = spark.createDataFrame(df)
# # COMMAND -----
# sparkdf.write.format("delta").mode("append").save(bronze_path + "testing.delta")
# COMMAND -----
dbutils.notebook.exit("SUCCESS")
