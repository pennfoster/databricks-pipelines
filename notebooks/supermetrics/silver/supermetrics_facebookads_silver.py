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

search_name = "FacebookAds"
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
        year string,
        yearmonth string,
        month string,
        dayofmonth string,
        dayofweek string,
        account string,
        accountid string,
        date date,
        campaignname string,
        campaignid string,
        campaignstatus string,
        campaigndailybudget float,
        campaignstartdate string,
        campaignenddate string,
        adsetname string,
        adsetid string,
        adsetstatus string,
        adsetstarttime string,
        adsetendtime string,
        adsettargeting string,
        adname string,
        adid string,
        adstatus string,
        destinationurl string,
        adurltags string,
        trackingtemplateurl string,
        adcalltoactiontype string,
        cost float,
        reach float,
        impressions float,
        cpmcostper1000impressions float,
        linkclicks float,
        uniquelinkclicks float,
        ctrlinkclickthroughrate string,
        cpccostperlinkclick float,
        costperuniquelinkclick float,
        threesecondvideoviews float,
        uniquethreesecondvideoviews float,
        thirtysecondvideoviews float,
        videowatchesat25percent float,
        videowatchesat50percent float,
        videowatchesat75percent float,
        videowatchesat95percent float,
        videowatchesat100percent float,
        videoaveragewatchtime float,
        clickstoplayvideo float,
        uniqueclickstoplayvideo float,
        avgcanvasviewtimeseconds float,
        avgcanvasviewpercentage string,
        costperthreesecondvideoview float,
        websiteconversions float,
        websiteconversionrate string,
        websitecustomconversions float,
        websiteleads float,
        websitepurchases float,
        costperwebsiteconversion float,
        costperwebsitecustomconversions float,
        costperwebsitelead float,
        costperwebsitepurchase float,
        videoplayactions float,
        frequency float,
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
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            day(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            profile as account,
            profileid as accountid,
            date,
            adcampaign_name as campaignname,
            adcampaign_id as campaignid,
            campaignstatus,
            try_cast(campaign_daily_budget as float) as campaigndailybudget,
            campaign_start_date as campaignstartdate,
            campaign_end_date as campaignenddate,
            adset_name as adsetname,
            adset_id as adsetid,
            adsetstatus,
            adsetstart_time as adsetstarttime,
            adsetend_time as adsetendtime,
            adsettargeting,
            ad_name as adname,
            ad_id as adid,
            adstatus,
            destinationurl,
            creative_url_tags as adurltags,
            creative_template_url as trackingtemplateurl,
            creative_call_to_action_type as adcalltoactiontype,
            try_cast(cost as float) as cost,
            try_cast(reach as float) as reach,
            try_cast(impressions as float) as impressions,
            try_cast(cpm as float) as cpmcostper1000impressions,
            try_cast(action_link_click as float) as linkclicks,
            try_cast(unique_action_link_click as float) as uniquelinkclicks,
            link_ctr as ctrlinkclickthroughrate,
            try_cast(cplc as float) as cpccostperlinkclick,
            try_cast(ucplc as float) as costperuniquelinkclick,
            try_cast(action_video_view as float) as threesecondvideoviews,
            try_cast(unique_action_video_view as float) as uniquethreesecondvideoviews,
            try_cast(video_30_sec_watched_actions as float) as thirtysecondvideoviews,
            try_cast(video_p25_watched_actions as float) as videowatchesat25percent,
            try_cast(video_p50_watched_actions as float) as videowatchesat50percent,
            try_cast(video_p75_watched_actions as float) as videowatchesat75percent,
            try_cast(video_p95_watched_actions as float) as videowatchesat95percent,
            try_cast(video_p100_watched_actions as float) as videowatchesat100percent,
            try_cast(video_average_watch_time as float) as videoaveragewatchtime,
            try_cast(action_video_play as float) as clickstoplayvideo,
            try_cast(unique_action_video_play as float) as uniqueclickstoplayvideo,
            try_cast(canvas_avg_view_time as float) as avgcanvasviewtimeseconds,
            canvas_avg_view_percent as avgcanvasviewpercentage,
            try_cast(cost_per_3s_video_view as float) as costperthreesecondvideoview,
            try_cast(offsite_conversions as float) as websiteconversions,
            offsite_conversion_rate as websiteconversionrate,
            try_cast(offsite_conversions_fb_pixel_custom as float) as websitecustomconversions,
            try_cast(offsite_conversions_fb_pixel_lead as float) as websiteleads,
            try_cast(offsite_conversions_fb_pixel_purchase as float) as websitepurchases,
            try_cast(cost_per_website_conversion as float) as costperwebsiteconversion,
            try_cast(cost_per_website_custom_conversion as float) as costperwebsitecustomconversions,
            try_cast(cost_per_website_lead as float) as costperwebsitelead,
            try_cast(cost_per_website_purchase as float) as costperwebsitepurchase,
            try_cast(video_play_actions as float) as videoplayactions,
            try_cast(frequency as float) as frequency,
            null as mkwid,
            case
              when profileid in ('632564820128578', '1731008223582165', '2268184246707247', '1617848434898145', '3191919400930025', '54037916', '108180593103030', '54041810')
                and year(date) >= 2020
                then reverse(split(ad_name, '-'))[1]
              else null
            end as pubcode,
            case
              when profileid in ('313484675751669')
                and year(date) >= 2020
                then reverse(split(ad_name, '-'))[1]
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
