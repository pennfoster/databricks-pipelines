# Databricks notebook source
import os
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from pytz import timezone

from data_sources.supermetrics.classes import Supermetrics
from data_sources.supermetrics.functions import get_url_dataframe, save_json, load_json
from shared.constants import COMPANY_TIMEZONE

# COMMAND -----
dbutils.widgets.dropdown("environment", "dev", ["dev", "prd"])
# dbutils.widgets.text("start_date", "")
# dbutils.widgets.text("end_date", "")

env = dbutils.widgets.get("environment")
url_df = get_url_dataframe(env)
search_names = url_df["C001_SearchName"].sort_values().unique().tolist()
search_names += [""]
dbutils.widgets.dropdown("search_name", "", search_names)

# COMMAND -----
search_name = dbutils.widgets.get("search_name")
# row = url_df[url_df["C001_QueryName"] == query_name]
# url = row["C001_URL"].values[0]
# search_name = row["C001_SearchName"].values[0]
# start_date = dbutils.widgets.get("start_date")
# end_date = dbutils.widgets.get("end_date")

# raw_dir = f"/mnt/sadataraw{env}001_landing/supermetrics/{search_name}/{query_name}"
bronze_dir = f"mnt/bronze/supermetrics/{search_name}/"

# COMMAND -----
# indx = 1
# row = df.iloc[1, :]
# search_name = row["C001_SearchName"]
# bronze_path = f"mnt/bronze/supermetrics/{search_name}"
paths = [i for i in Path(os.path.join("/dbfs", bronze_dir)).iterdir() if i.is_dir()]
queries = [i.parts[-1] for i in paths]
delta_dirs = [i.joinpath(j) for i, j in zip(paths, queries)]

# p = Path("/mnt/bronze/supermetrics/FacebookAdSetV2")
# COMMAND -----
# TESTING
# TODO: Remove testing cell
p = paths[0]
d = delta_dirs[0]


# dir = paths[0]
# dir_mnt = Path(*dir.parts[2:])
spark.sql(
    f"""
    select * 
    from delta.`{'/' / Path(*d.parts[2:])}`
    
"""
spark.read.option("startingVersion", "latest").load(c.as_posix()).display()
)

  spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_{SOURCE}_{GROUP}.table_column_mapping (bronze_table STRING, bronze_column STRING, silver_table STRING, silver_column STRING, primary_key BOOLEAN, convert_julian_date BOOLEAN) PARTITIONED BY (bronze_table) LOCATION '/mnt/sadatalakehouse{ENV}001_bronze/{SOURCE}/{GROUP}/table_column_mapping'")
# %sql
# select
#   replace(customurlparameters, '"', '') as customurlparameters
#   , year(date) as year
#   , year(date) || '|' || month(date) as yearmonth
#   , month(date) as month
#   , day(date) as dayofmonth
#   , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
#   , customurlparameters:mkwid as mkwid
#   , customurlparameters:dskeyname as mkwid
#   , customurlparameters:pubcode as pubcode
#   , customurlparameters:adkey as adkey
#   , customurlparameters:dskeyword as dskeyword
#   , * except(customurlparameters)
# from
#   bronze_supermetrics.testing_testingfirsttest
# order by
#   2
