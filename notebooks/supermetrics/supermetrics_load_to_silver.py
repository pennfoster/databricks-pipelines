# Databricks notebook source
# %pip install aiohttp paramiko
# COMMAND -----
import os
import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path
from pytz import timezone

from data_sources.supermetrics.classes import Supermetrics
from shared.classes.table_schemas import TableSchemas
from shared.functions.azure_utilities import get_mount_paths
from data_sources.supermetrics.functions import get_url_dataframe, save_json, load_json
from shared.constants import COMPANY_TIMEZONE

spark.conf.set("spark.sql.ansi.enabled", True)
# COMMAND -----
tz = timezone(COMPANY_TIMEZONE)
seven_days_ago = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")

dbutils.widgets.text("end_date", datetime.now(tz).strftime("%Y-%m-%d"), "")
dbutils.widgets.combobox("start_date", seven_days_ago, [seven_days_ago, "1900-01-01"])

url_df = get_url_dataframe()
search_names = url_df["C001_SearchName"].sort_values().unique().tolist()
search_names += [""]

dbutils.widgets.dropdown("search_name", "", search_names)

# COMMAND -----
search_name = dbutils.widgets.get("search_name")
if not search_name:
    raise ValueError("Empty search_name parameter provided.")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

where_clause_start = f"where date >= '{start_date}'"
if end_date:
    where_clause_end = f"and date <= '{end_date}'"


data_source = "supermetrics"
silver_db = f"silver_{data_source}"
bronze_db = f"bronze_{data_source}"
paths = get_mount_paths(data_source)
bronze_dir = f"{paths.bronze}/{search_name.lower()}/"
silver_dir = f"{paths.silver}/{search_name.lower()}"


# COMMAND -----
dirs = [i for i in Path("/dbfs" + bronze_dir).iterdir() if i.is_dir()]
queries = [i.name for i in dirs]
delta_dirs = [i.joinpath(j) for i, j in zip(dirs, queries)]
if not delta_dirs:
    raise ValueError(
        f"No bronze tables were found in the bronze directory provided: {bronze_dir}"
    )
# COMMAND -----
spark.sql(f"create database if not exists {silver_db}")

for i in delta_dirs:
    query_name = i.name
    table_dir = "/" / Path(*i.parts[2:])
    bronze_table_path = f"{bronze_db}.{search_name.lower()}_{query_name.lower()}"

    history = spark.sql(
        f"describe history {bronze_db}.{search_name.lower()}_{query_name.lower()}"
    ).toPandas()
    latest_version = history["version"].max()

    ts = TableSchemas(data_source=data_source, table_name=search_name.lower())
    select_clause = ts.get_select_clause(nullif="")
    query = f"""
        select
          '{query_name}' as queryname
          , year(date) as year
          , year(date) || '|' || month(date) as yearmonth
          , month(date) as month
          , day(date) as dayofmonth
          , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
          , {select_clause}
        from
          {bronze_db}.{search_name.lower()}_{query_name.lower()}@v{latest_version}
    """
    # --{where_clause_start} -- these are for filtering dates to not rebuild table each time
    # --{where_clause_end} -- this is for filtering dates to not rebuild table each time
    spdf = spark.sql(query)

    # pddf = spdf.toPandas()
    # dates = pddf.date.unique().tolist()
    # spark.sql(f'''
    #     delete from {silver_db}.{search_name}
    #     where dates in ({dates})
    #     and lower(queryname) = lower({query_name})
    # ''')

    spdf.write.format("delta").mode("overwrite").option("mergeSchema", True).option(
        "overwriteSchema", True
    ).option("replaceWhere", f"queryname = '{query_name}'").partitionBy(
        "queryname"
    ).save(
        f"{silver_dir}"
    )

spark.sql(
    f"""
    create table if not exists {silver_db}.{search_name.lower()} location '{silver_dir}'
"""
)

# COMMAND -----
