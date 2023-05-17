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

tz = timezone(COMPANY_TIMEZONE)
seven_days_ago = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")

dbutils.widgets.text("end_date", datetime.now(tz).strftime("%Y-%m-%d"), "")
dbutils.widgets.combobox("start_date", seven_days_ago, [seven_days_ago, "1900-01-01"])

search_name = "BingKeyword"
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
        queryname string,
        date date,
        keyword string,
        customurlparameters string,
        year string,
        yearmonth string,
        month string,
        dayofmonth string,
        dayofweek string,
        accountname string,
        accountid string,
        accountnumber string,
        campaignname string,
        campaignid string,
        status string,
        adgroupname string,
        adgroupid string,
        adgroupstatus string,
        finalurl string,
        finalmobileurl string,
        deliveredmatchtype string,
        keywordid string,
        keywordstatus string,
        currentmaxcpc decimal(9, 2),
        campaignlabels string,
        adgrouplabels string,
        keywordlabels string,
        impressions int,
        clicks int,
        cost decimal(9, 2),
        ctrpercent float,
        cpc decimal(9, 2),
        cpm decimal(9, 2),
        averageposition float,
        qualityscore int,
        conversions int,
        conversionrate string,
        conversionsperimpressionrate string,
        costperconversion decimal(9, 2),
        revenueperconversion decimal(9, 2),
        revenue decimal(9, 2),
        returnonadspend float,
        mkwid string,
        pubcode string,
        adkey string,
        _record_insert_date timestamp
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
            date,
            keyword,
            customparameters as customurlparameters,
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            accountname,
            accountid,
            accountnumber,
            campaignname,
            campaignid,
            campaignstatus as status,
            adgroupname,
            adgroupid,
            adgroupstatus,
            finalurl,
            finalmobileurl,
            bidmatchtype as deliveredmatchtype,
            keywordid,
            keywordstatus,
            currentmaxcpc,
            campaignlabels,
            adgrouplabels,
            keywordlabels,
            try_cast(impressions as int) as impressions,
            try_cast(clicks as int) as clicks,
            try_cast(spend as decimal(9, 2)) as cost,
            try_cast(ctr as float) as ctrpercent,
            try_cast(cpc as decimal(9, 2)) as cpc,
            try_cast(cpm as decimal(9, 2)) as cpm,
            try_cast(averageposition as float) as averageposition,
            try_cast(qualityscore as int) as qualityscore,
            try_cast(conversions as int) as conversions,
            conversionrate,
            cpi as conversionsperimpressionrate,
            try_cast(costperconversion as decimal(9, 2)) as costperconversion,
            try_cast(revenueperconversion as decimal(9, 2)) as revenueperconversion,
            try_cast(revenue as decimal(9, 2)) as revenue,
            try_cast(roas as float) as returnonadspend,
            case 
              when contains(customparameters, '_mkwid') then split(split(customparameters, '_mkwid}}=')[1], ';')[0] 
              else null
            end as mkwid,
            case 
              when contains(customparameters, '_pubcode') then split(split(customparameters, '_pubcode}}=')[1], ';')[0] 
              else null 
            end as pubcode,
            case 
              when contains(customparameters, '_adkey') then split(split(customparameters, '_adkey}}=')[1], ';')[0] 
              else null 
            end as adkey,
            _record_insert_date
        from
            {bronze_db}.{search_name.lower()}_{i.lower()}@v{latest_version}
        {where_clause_start}
        {where_clause_end}
    """
    select_queries.append(select_query)

if not select_queries:
    dbutils.notebook.exit(f"No table found for {bronze_db}.{search_name}_{i.lower()}.")

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
