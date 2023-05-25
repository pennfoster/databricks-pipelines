# Databricks notebook source
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

search_name = "GoogleSearchAds"
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
        customurlparameters string,
        account string,
        accountid string,
        network string,
        campaignname string,
        campaignid string,
        campaignstatus string,
        adgroupname string,
        adgroupid string,
        adgroupstatus string,
        headline string,
        headlinepart1 string,
        headlinepart2 string,
        headlinepart3 string,
        path1 string,
        path2 string,
        description string,
        description1 string,
        description2 string,
        expandedtextaddescription1 string,
        expandedtextaddescription2 string,
        adtype string,
        adstatus string,
        finalurl string,
        trackingurltemplate string,
        campaignlabels string,
        adgrouplabels string,
        adlabels string,
        labels string,
        impressions int,
        clicks int,
        cost decimal(38, 4),
        ctr string,
        cpc decimal(38, 4),
        cpm decimal(38, 4),
        conversions decimal(38, 4),
        conversionrate string,
        conversionsperimpression string,
        costperconversion decimal(38, 4),
        valueperconversion decimal(38, 4),
        totalconversionvalue decimal(38, 4),
        returnonadspend decimal(38, 4),
        viewthroughconversions decimal(38, 4),
        allconversions decimal(38, 4),
        allconversionvalue decimal(38, 4),
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

    query_version = url_df[url_df["C001_QueryName"] == i]["QueryNameVersion"].values[0]
    query_version = f"V{query_version}" if query_version > 1 else ""

    select_query = f"""
        select
            '{i + query_version}' as queryname,
            date as date,
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            dayofweek(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            adid,
            customurlparameters,
            profile,
            profileid,
            network,
            campaignname,
            campaignid,
            campaignstatus,
            adgroupname,
            adgroupid,
            adgroupstatus,
            headline::string,
            headlinepart1,
            headlinepart2,
            headlinepart3,
            path1,
            path2,
            description,
            description1,
            description2,
            textaddescription1 as expandedtextaddescription1,
            textaddescription2 as expandedtextaddescription2,
            adtype,
            adstatus,
            finalurl,
            trackingurltemplate::string,
            campaignlabels,
            adgrouplabels::string,
            adlabels,
            labels,
            impressions::int as impressions,
            clicks::int as clicks,
            cost::decimal(38, 4) as cost,
            ctr,
            cpc::decimal(38, 4) as cpc,
            cpm::decimal(38, 4) as cpm,
            conversions::decimal(38, 4) as conversions,
            conversionrate,
            cpi as conversionsperimpression,
            costperconversion::decimal(38, 4) as costperconversion,
            valueperconvmanyperclick::decimal(38, 4) as valueperconversion,
            conversionvalue::decimal(38, 4) as totalconversionvalue,
            roas::decimal(38, 4) as returnonadspend,
            viewthroughconversions::decimal(38, 4) as viewthroughconversions,
            estimatedtotalconversions::decimal(38, 4) as allconversions,
            estimatedtotalconversionvalue::decimal(38, 4) as allconversionvalue,
            customurlparameters:mkwid,
            customurlparameters:pubcode,
            customurlparameters:adkey,
            null as landingpageurl,
            _record_insert_date
        from
            {bronze_db}.{search_name.lower()}_{i.lower()}@v{latest_version}
        {where_clause_start}
        {where_clause_end}
        qualify
          dense_rank() over(partition by date order by _record_insert_date desc) = 1
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
