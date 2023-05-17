# Databricks notebook source
# %pip install aiohttp paramiko
# COMMAND -----
from datetime import datetime, timedelta
from pytz import timezone

from shared.functions.azure_utilities import get_mount_paths
from data_sources.supermetrics.functions import get_url_dataframe
from shared.constants import COMPANY_TIMEZONE

spark.conf.set("spark.sql.ansi.enabled", True)

tz = timezone(COMPANY_TIMEZONE)
seven_days_ago = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")

dbutils.widgets.text("end_date", datetime.now(tz).strftime("%Y-%m-%d"), "")
dbutils.widgets.combobox("start_date", seven_days_ago, [seven_days_ago, "1900-01-01"])

search_name = "BingSearchAds"
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
        year string,
        yearmonth string,
        month string,
        dayofmonth string,
        dayofweek string,
        adid string,
        customparameters string,
        accountname string,
        accountid string,
        accountnumber string,
        campaignname string,
        campaignid string,
        status string,
        adgroupname string,
        adgroupid string,
        adgroupstatus string,
        adtitle string,
        adtitlepart1 string,
        adtitlepart2 string,
        adtitlepart3 string,
        adpathpart1 string,
        adpathpart2 string,
        addescription string,
        addescription2 string,
        adtype string,
        adstatus string,
        finalurl string,
        displayurl string,
        trackingtemplate string,
        campaignlabels string,
        adgrouplabels string,
        adlabels string,
        impressions float,
        clicks float,
        cost float,
        ctrpercentage float,
        cpc float,
        cpm float,
        averageposition float,
        conversions float,
        conversionrate string,
        conversionsperimpressionrate string,
        costperconversion float,
        revenueperconversion float,
        revenue float,
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
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            adid,
            customparameters,
            accountname,
            accountid,
            accountnumber,
            campaignname,
            campaignid,
            campaignstatus as status,
            adgroupname,
            adgroupid,
            adgroupstatus,
            adtitle,
            adtitlepart1,
            adtitlepart2,
            adtitlepart3,
            adpath1 as adpathpart1,
            adpath2 as adpathpart2,
            addescription,
            addescription2,
            adtype,
            adstatus,
            finalurl,
            displayurl,
            trackingtemplate,
            campaignlabels,
            adgrouplabels,
            adlabels,
            try_cast(impressions as float) as impressions,
            try_cast(clicks as float) as clicks,
            try_cast(spend as float) as cost,
            try_cast(ctr as float) as ctrpercentage,
            try_cast(cpc as float) as cpc,
            try_cast(cpm as float) as cpm,
            try_cast(averageposition as float) as averageposition,
            try_cast(conversions as float) as conversions,
            conversionrate,
            cpi as conversionsperimpressionrate,
            try_cast(costperconversion as float) as costperconversion,
            try_cast(revenueperconversion as float) as revenueperconversion,
            try_cast(revenue as float) as revenue,
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
