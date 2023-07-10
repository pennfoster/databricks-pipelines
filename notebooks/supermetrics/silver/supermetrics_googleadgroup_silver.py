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

search_name = "GoogleAdGroup"
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
        adgroupname string,
        adgroupid string,
        account string,
        accountid string,
        network string,
        campaignname string,
        campaignid string,
        campaignstatus string,
        biddingstrategy string,
        adgroupstatus string,
        maxcpa string,
        maxcpm string,
        customurlparameters string,
        campaignlabels string,
        adgrouplabels string,
        labels string,
        impressions decimal(38, 4),
        clicks decimal(38, 4),
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
        videoimpressions decimal(38, 4),
        videoviews decimal(38, 4),
        phonecallimpressions decimal(38, 4),
        calls decimal(38, 4),
        callrate string,
        gmailforwards decimal(38, 4),
        gmailsaves decimal(38, 4),
        gmailsecondaryclicks decimal(38, 4),
        impressionshare decimal(38, 4),
        searchimpressionshare string,
        availableimpressions decimal(38, 4),
        topimpressionpercentage string,
        absolutetopimpressionpercentage string,
        searchabsolutetopimpressionshare string,
        searchtopimpressionshare string,
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
            adgroupname,
            adgroupid,
            profile as account,
            profileid as accountid,
            network,
            campaignname,
            campaignid,
            campaignstatus,
            biddingstrategytype as biddingstrategy,
            adgroupstatus,
            maxcpa1perclick as maxcpa,
            maxcpm,
            customurlparameters,
            campaignlabels,
            adgrouplabels::string,
            labels::string,
            ifnull(impressions::decimal(38, 4), 0) as impressions,
            ifnull(clicks::decimal(38, 4), 0) as clicks,
            ifnull(cost::decimal(38, 4), 0) as cost,
            ctr,
            ifnull(cpc::decimal(38, 4), 0) as cpc,
            ifnull(cpm::decimal(38, 4), 0) as cpm,
            ifnull(conversions::decimal(38, 4), 0) as conversions,
            conversionrate,
            cpi,
            ifnull(costperconversion::decimal(38, 4), 0) as costperconversion,
            ifnull(valueperconvmanyperclick::decimal(38, 4), 0) as valueperconversion,
            ifnull(conversionvalue::decimal(38, 4), 0) as totalconversionvalue,
            ifnull(roas::decimal(38, 4), 0) as returnonadspend,
            ifnull(viewthroughconversions::decimal(38, 4), 0) as viewthroughconversions,
            ifnull(estimatedtotalconversions::decimal(38, 4), 0) as allconversions,
            ifnull(estimatedtotalconversionvalue::decimal(38, 4), 0) as allconversionvalue,
            ifnull(videoimpressions::decimal(38, 4), 0) as videoimpressions,
            ifnull(videoviews::decimal(38, 4), 0) as videoviews,
            ifnull(numofflineimpressions::decimal(38, 4), 0) as phonecallimpressions,
            ifnull(calls::decimal(38, 4), 0) as calls,
            callrate,
            ifnull(gmailforwards::decimal(38, 4), 0) as gmailforwards,
            ifnull(gmailsaves::decimal(38, 4), 0) as gmailsaves,
            ifnull(gmailsecondaryclicks::decimal(38, 4), 0) as gmailsecondaryclicks,
            ifnull(impressionshare::decimal(38, 4), 0) as impressionshare,
            searchimpressionshare,
            ifnull(totalavailableimpressions::decimal(38, 4), 0) as availableimpressions,
            topimpressionpercentage,
            absolutetopimpressionpercentage,
            searchabsolutetopimpressionshare,
            searchtopimpressionshare,
            case 
              when contains(customurlparameters, '_mkwid') then split(split(customurlparameters, '_mkwid}}=')[1], ';')[0] 
              else null
            end as mkwid,
            case 
              when contains(customurlparameters, '_pubcode') then split(split(customurlparameters, '_pubcode}}=')[1], ';')[0] 
              else null 
            end as pubcode,
            case 
              when contains(customurlparameters, '_adkey') then split(split(customurlparameters, '_adkey}}=')[1], ';')[0] 
              else null 
            end as adkey,
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
