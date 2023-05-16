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

search_name = "BingCampaign"
url_df = get_url_dataframe()
query_list = (
    url_df[url_df["C001_SearchName"] == search_name]["C001_QueryName"]
    .sort_values()
    .to_list()
)
query_list += ["All"]
dbutils.widgets.dropdown("query_name", "All", query_list)
# COMMAND -----
query_name = dbutils.widgets.get("query_name")
if query_name == "All":
    query_list.remove("All")
else:
    query_list = [query_name]

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
spark.sql(f"create database if not exists {silver_db}")

create_query = f"""
    create table if not exists {silver_db}.{search_name} (
      queryname varchar(250)
      , date date
      , year varchar(255)
      , yearmonth varchar(255)
      , month varchar(255)
      , dayofmonth varchar(255)
      , dayofweek varchar(255)
      , campaignname varchar(255)
      , campaignid varchar(255)
      , accountname varchar(255)
      , accountid varchar(255)
      , accountnumber varchar(255)
      , status varchar(255)
      , campaigntype varchar(255)
      , campaignlabels varchar(255)
      , impressions varchar(255)
      , clicks float
      , cost float
      , ctrpercentage float
      , cpc float
      , cpm float
      , averageposition float
      , conversions float
      , conversionrate varchar(255)
      , conversionsperimpressionrate varchar(255)
      , costperconversion float
      , RevenuePerConversion float
      , revenue float
      , returnonadspend float
      , campaignvertical varchar(255)
      , _record_insert_date timestamp
    )
    using delta
    location '{silver_dir}'
    ;
"""
spark.sql(create_query)
# COMMAND -----
select_queries = []

for i in query_list:
    tables = spark.catalog.listTables(bronze_db)
    tables = [x.name for x in tables]
    if f"{search_name.lower()}_{i.lower()}" not in tables:
        continue

    history = spark.sql(
        f"describe history {bronze_db}.{search_name.lower()}_{i.lower()}"
    ).toPandas()
    latest_version = history["version"].max()
    select_query = f"""
        select
            '{i}' as queryname,
            try_cast(date as date) as date,
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            try_cast(campaignname as varchar(255)) as campaignname,
            try_cast(campaignid as varchar(255)) as campaignid,
            try_cast(accountname as varchar(255)) as accountname,
            try_cast(accountid as varchar(255)) as accountid,
            try_cast(accountnumber as varchar(255)) as accountnumber,
            try_cast(campaignstatus as varchar(255)) as status,
            try_cast(campaigntype as varchar(255)) as campaigntype,
            try_cast(campaignlabels as varchar(255)) as campaignlabels,
            try_cast(impressions as float) as impressions,
            try_cast(clicks as float) as clicks,
            -- try_cast(cost as float) as cost,
            null as cost,
            -- try_cast(ctrpercentage as float) as,
            null as ctrpercentage,
            try_cast(cpc as float) as cpc,
            try_cast(cpm as float) as cpm,
            try_cast(averageposition as float) as averageposition,
            try_cast(conversions as float) as conversions,
            try_cast(conversionrate as varchar(255)) as conversionrate,
            -- try_cast(conversionsperimpressionrate as varchar(255)) as conversionsperimpressionrate,
            null as conversionsperimpressionrate,
            try_cast(CostPerConversion as float) as costperconversion,
            try_cast(RevenuePerConversion as float) as revenueperconversion,
            try_cast(revenue as float) as revenue,
            -- try_cast(returnonadspend as float) as returnonadspend,
            null as returnonadspend,
            reverse(split(campaignname, '-'))[0] as campaignvertical,
            try_cast(_record_insert_date as timestamp) as _record_insert_date
        from
            {bronze_db}.{search_name.lower()}_{i.lower()}@v{latest_version}
        {where_clause_start}
        {where_clause_end}
    """
    select_queries.append(select_query)

# COMMAND -----
df = spark.sql(" union ".join(select_queries))
df.createOrReplaceTempView(f"{search_name.lower()}_tmp")
spark.sql(
    f"""
    delete from {silver_db}.{search_name.lower()}
    where date in (
        select distinct date from {search_name.lower()}_tmp
    )
    ;
"""
)
spark.sql(
    f"""
   insert into {silver_db}.{search_name.lower()}
   select * from {search_name.lower()}_tmp
   ;
"""
)

# COMMAND -----
dbutils.notebook.exit("SUCCESS")
