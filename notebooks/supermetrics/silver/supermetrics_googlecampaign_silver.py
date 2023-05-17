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

search_name = "GoogleCampaign"
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
        campaignname string,
        campaignid string,
        account string,
        accountid string,
        network string,
        campaignstatus string,
        servingstatus string,
        advertisingchanneltype string,
        advertisingchannelsubtype string,
        startdate string,
        enddate string,
        biddingstrategy string,
        campaigndesktopbidmodifier string,
        campaignmobilebidmodifier string,
        campaigntabletbidmodifier string,
        campaignlabels string,
        impressions float,
        clicks float,
        cost float,
        ctr string,
        cpc float,
        cpm float,
        budget float,
        conversions float,
        conversionrate string,
        conversionsperimpression string,
        costperconversion float,
        valueperconversion float,
        totalconversionvalue float,
        returnonadspend float,
        viewthroughconversions float,
        allconversions float,
        allconversionvalue float,
        videoimpressions float,
        videoviews float,
        phonecallimpressions float,
        calls float,
        callrate string,
        gmailforwards float,
        gmailsaves float,
        gmailsecondaryclicks float,
        impressionshare float,
        searchimpressionshare string,
        availableimpressions float,
        topimpressionpercentage string,
        absolutetopimpressionpercentage string,
        searchabsolutetopimpressionshare string,
        searchtopimpressionshare string,
        campaignverticle string,
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
            date as date,
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            campaignname,
            campaignid,
            profile,
            profileid,
            network,
            campaignstatus,
            campaignservingstatus as servingstatus,
            advertisingchanneltype,
            advertisingchannelsubtype,
            startdate,
            enddate,
            biddingstrategytype as biddingstrategy,
            campaigndesktopbidmodifier,
            campaignmobilebidmodifier,
            campaigntabletbidmodifier,
            campaignlabels,
            try_cast(impressions as float) as impressions,
            try_cast(clicks as float) as clicks,
            try_cast(cost as float) as cost,
            try_cast(ctr as string) as ctr,
            try_cast(cpc as float) as cpc,
            try_cast(cpm as float) as cpm,
            try_cast(budget as float) as budget,
            try_cast(conversions as float) as conversions,
            conversionrate,
            cpi as conversionsperimpression,
            try_cast(costperconversion as float) as costperconversion,
            try_cast(valueperconvmanyperclick as float) as valueperconversion,
            try_cast(conversionvalue as float) as totalconversionvalue,
            try_cast(roas as float) as returnonadspend,
            try_cast(viewthroughconversions as float) as viewthroughconversions,
            try_cast(estimatedtotalconversions as float) as allconversions,
            try_cast(estimatedtotalconversionvalue as float) as allconversionvalue,
            try_cast(videoimpressions as float) as videoimpressions,
            try_cast(videoviews as float) as videoviews,
            try_cast(numofflineimpressions as float) as phonecallimpressions,
            try_cast(calls as float) as calls,
            callrate,
            try_cast(gmailforwards as float) as gmailforwards,
            try_cast(gmailsaves as float) as gmailsaves,
            try_cast(gmailsecondaryclicks as float) as gmailsecondaryclicks,
            try_cast(impressionshare as float) as impressionshare,
            searchimpressionshare,
            try_cast(totalavailableimpressions as float) as availableimpressions,
            topimpressionpercentage,
            absolutetopimpressionpercentage,
            searchabsolutetopimpressionshare,
            searchtopimpressionshare,
            reverse(split(campaignname, '-'))[0] as campaignvertical,
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
