# Databricks notebook source
%pip install aiohttp paramiko
# COMMAND -----
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from pytz import timezone

from pyspark.sql.functions import current_timestamp, input_file_name

from data_sources.supermetrics.classes import Supermetrics
from data_sources.supermetrics.functions import get_url_dataframe, save_json, load_json
from shared.functions.azure_utilities import get_mount_paths

spark.conf.set("spark.sql.ansi.enabled")
# COMMAND -----
dbutils.widgets.dropdown("environment", "dev", ["dev", "prd"])
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")

# env = dbutils.widgets.get("environment")
url_df = get_url_dataframe()
queries = url_df["C001_QueryName"].sort_values().to_list()
queries += [""]
dbutils.widgets.dropdown("query_name", "", queries)

# COMMAND -----
query_name = dbutils.widgets.get("query_name")
if not query_name:
    raise ValueError("Empty query_name parameter provided.")
row = url_df[url_df["C001_QueryName"] == query_name]
url = row["C001_URL"].values[0]
# url = url.replace("/json?", "/keyjson?")
search_name = row["C001_SearchName"].values[0]
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

if start_date == "":
    date_range = None
elif end_date == "":
    date_range = f'"date_range_type":"{start_date}",'
else:
    date_range = f'"start_date":"{start_date}","end_date":"{end_date}",'
if date_range:
    url = re.sub('(?<=)"date_range.+?(?<=\,)', date_range, url)

data_source = "supermetrics"
paths = get_mount_paths("supermetrics")
landing_dir = f"{paths.landing}/{search_name}/{query_name}"
bronze_dir = f"{paths.bronze}/{search_name}/{query_name}"

# COMMAND -----
sm = Supermetrics()
resp_json = sm.get_data(url)
if not resp_json:
    print("No data retrived from API.")
    dbutils.notebook.exit("No data retrived from API.")
# keyjson = sm.get_keyjson(resp_json)


# COMMAND -----
json_path = save_json(
    dest_dir=Path(f"/dbfs/{landing_dir}"),
    file_name=f"{query_name}",
    data=resp_json,
    suffix="timestamp",
    parents=True,
)

print(json_path)

# COMMAND -----
# Load from raw and save to bronze
spdf = spark.read.json(landing_dir)
spdf.toDF(*[c.lower() for c in spdf.columns])
sparkdf = sparkdf.withColumn(
    "_etl_record_insert_date", current_timestamp()
).withColumn("_etl_source_file_path", lit(json_path))
sparkdf.write.format("delta").mode("append").partitionBy("date").option(
    "mergeSchema", True
).option("overwriteSchema", True).save(f"{bronze_dir}/{query_name}")

unprocessed = [
    str(file) for file in Path(f"/dbfs/{landing_dir}").iterdir() if file.is_file()
]

for file in unprocessed:
    # resp_json = load_json(f"{file}")
    # sm = Supermetrics()
    # sm.set_metadata(resp_json)

    # cols = [i["field_id"].lower() for i in sm.fields]
    # data = resp_json["data"][1:]

    # df = pd.DataFrame(columns=cols, data=data)
    # df = df.applymap(str)

    sparkdf = spark.createDataFrame(df)
    sparkdf = sparkdf.withColumn(
        "_etl_record_insert_date", current_timestamp()
    ).withColumn("_etl_source_file_path", input_file_name())
    sparkdf.write.format("delta").mode("append").partitionBy("date").option(
        "mergeSchema", True
    ).option("overwriteSchema", True).save(f"{bronze_dir}/{query_name}")

    spark.sql(
        f"""
        create table if not exists bronze_supermetrics.{search_name}_{query_name}
        using delta
        location '{bronze_dir}/{query_name}'
    """
    )

    processed_dir = f"{raw_dir}/processed"
    Path(f"/dbfs/{processed_dir}").mkdir(parents=False, exist_ok=True)
    dbutils.fs.mv(f"{file}".replace("/dbfs", ""), processed_dir)

# COMMAND -----

# COMMAND -----
dbutils.notebook.exit("SUCCESS")


# dbutils.notebook.exit("SUCCESS")


# from pyspark.sql.types import *
# dtype_map = {
#     'string.time.date': StringType(),
#     'string.text.value': StringType(),
#     'int.number.value': StringType(),
#  'float.currency.value': StringType(),
#  'float.number.ratio': StringType(),
#  'float.number.value': StringType()}

# fields = b['meta']['query']['fields']
# data = b['data'][1:]
# a = []
# for i in fields:
#     a.append(StructField(
#         name=i['field_id'].lower(),
#         dataType=dtype_map[i['data_type']],
#         nullable=True
#     ))
# schema = StructType(a)
# df = spark.createDataFrame(data=data, schema=schema)

# from pyspark.sql.functions import col
# df_temp.select(*(col(c).cast("float").alias(c) for c in df_temp.columns))
# df.withColumn("salary",col("salary").cast('double'))




resp_json = [{
[{
  "RecordNumber": 2,
  "Zipcode": 704,
  "ZipCodeType": "STANDARD",
  "City": "PASEO COSTA DEL SUR",
  "State": "PR"
},
{
  "RecordNumber": 10,
  "Zipcode": 709,
  "ZipCodeType": "STANDARD",
  "City": "BDA SAN LUIS",
  "State": "PR"
}]
}]
resp_json2 = [{
  "RecordNumber": 2.0,
  "Zipcode": 704.234,
  "ZipCodeType": "STANDARD",
  "City": "PASEO COSTA DEL SUR",
  "State": "PR"
},
{
  "RecordNumber": '',
  "Zipcode": 709,
  "ZipCodeType": "STANDARD",
  "City": "BDA SAN LUIS",
  "State": "PR"
}]
json_path = save_json(
    dest_dir=Path(f"/dbfs/{landing_dir}"),
    file_name=f"{query_name}",
    data=resp_json,
    suffix="timestamp",
    parents=True,
)

json_path = save_json(
    dest_dir=Path(f"/dbfs/{landing_dir}"),
    file_name=f"{query_name}",
    data=resp_json2,
    suffix="timestamp",
    parents=True,
)

print(json_path)