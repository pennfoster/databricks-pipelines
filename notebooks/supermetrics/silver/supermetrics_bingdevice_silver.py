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

search_name = "BingDevice"
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
        devicetype string,
        keyword string,
        keywordid string,
        adid string,
        adgroupname string,
        adgroupid string,
        campaignname string,
        campaignid string,
        accountname string,
        accountid string,
        accountnumber string,
        status string,
        campaigntype string,
        adgroupstatus string,
        adtype string,
        adstatus string,
        impressions decimal(38, 4),
        clicks decimal(38, 4),
        cost decimal(38, 4),
        ctrpercent decimal(38, 4),
        cpc decimal(38, 4),
        cpm decimal(38, 4),
        averageposition decimal(38, 4),
        conversions decimal(38, 4),
        conversionrate string,
        conversionsperimpressionrate string,
        costperconversion decimal(38, 4),
        revenueperconversion decimal(38, 4),
        revenue decimal(38, 4),
        returnonadspend decimal(38, 4),
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
            devicetype string,
            keyword string,
            keywordid string,
            adid string,
            adgroupname string,
            adgroupid string,
            campaignname string,
            campaignid string,
            accountname string,
            accountid string,
            accountnumber string,
            campaignstatus as status,
            campaigntype string,
            adgroupstatus string,
            adtype string,
            adstatus string,
            impressions::decimal(38, 4) as impressions,
            clicks::decimal(38, 4) as clicks,
            spend::decimal(38, 4) as cost,
            ctr::decimal(38, 4) as ctrpercent,
            cpc::decimal(38, 4) as cpc,
            cpm::decimal(38, 4) as cpm,
            nullif(averageposition, 'None')::decimal(38, 4) as averageposition,
            conversions::decimal(38, 4) as conversions,
            conversionrate string,
            cpi::decimal(38, 4) as conversionsperimpressionrate,
            costperconversion::decimal(38, 4) as costperconversion,
            revenueperconversion::decimal(38, 4) as revenueperconversion,
            revenue::decimal(38, 4) as revenue,
            roas::decimal(38, 4) as returnonadspend,
            _record_insert_date timestamp
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
