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

search_name = "GoogleKeyword"
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
        account string,
        accountid string,
        campaignname string,
        campaignid string,
        campaignstatus string,
        biddingstrategy string,
        adgroupname string,
        adgroupid string,
        adgroupstatus string,
        finalurl string,
        finalmobileurl string,
        trackingurltemplate string,
        matchtype string,
        keywordid string,
        keywordstatus string,
        qualityscore string,
        firstpagecpc string,
        topofpagecpc string,
        firstpositioncpc string,
        campaignlabels string,
        adgrouplabels string,
        labels string,
        impressions string,
        clicks string,
        cost string,
        ctr string,
        cpc string,
        cpm string,
        conversions string,
        conversionrate string,
        conversionsperimpression string,
        costperconversion string,
        valueperconversion string,
        totalconversionvalue string,
        returnonadspend string,
        viewthroughconversions string,
        allconversions string,
        allconversionvalue string,
        impressionshare string,
        searchimpressionshare string,
        availableimpressions string,
        topimpressionpercentage string,
        absolutetopimpressionpercentage string,
        searchabsolutetopimpressionshare string,
        searchtopimpressionshare string,
        mkwid string,
        pubcode string,
        adkey string,
        landingpageurl string,
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
            keyword,
            customurlparameters,
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            profile,
            profileid,
            campaignname,
            campaignid,
            campaignstatus,
            biddingstrategytype,
            adgroupname,
            adgroupid,
            adgroupstatus,
            finalurl,
            finalmobileurls as finalmobileurl,
            trackingurltemplate,
            matchtype,
            keywordid,
            keywordstatus,
            qualityscore,
            firstpagecpc,
            topofpagecpc,
            firstpositioncpc,
            campaignlabels,
            adgrouplabels,
            labels,
            impressions,
            clicks,
            cost,
            ctr,
            cpc,
            cpm,
            conversions,
            conversionrate,
            cpi as conversionsperimpression,
            costperconversion,
            valueperconvmanyperclick as valueperconversion,
            conversionvalue as totalconversionvalue,
            roas as returnonadspend,
            viewthroughconversions,
            estimatedtotalconversions as allconversions,
            estimatedtotalconversionvalue as allconversionvalue,
            impressionshare,
            searchimpressionshare,
            totalavailableimpressions as availableimpressions,
            topimpressionpercentage,
            absolutetopimpressionpercentage,
            searchabsolutetopimpressionshare,
            searchtopimpressionshare,
            customurlparameters:mkwid as mkwid,
            customurlparameters:pubcode as pubcode,
            customurlparameters:adkey as adkey,
            null as landingpageurl,
            _record_insert_date
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
