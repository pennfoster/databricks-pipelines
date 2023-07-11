# Databricks notebook source

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
        campaigndailybudget decimal(38, 4),
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
        cost decimal(38, 4),
        reach decimal(38, 4),
        impressions decimal(38, 4),
        cpmcostper1000impressions decimal(38, 4),
        linkclicks decimal(38, 4),
        uniquelinkclicks decimal(38, 4),
        ctrlinkclickthroughrate string,
        cpccostperlinkclick decimal(38, 4),
        costperuniquelinkclick decimal(38, 4),
        threesecondvideoviews decimal(38, 4),
        uniquethreesecondvideoviews decimal(38, 4),
        thirtysecondvideoviews decimal(38, 4),
        videowatchesat25percent decimal(38, 4),
        videowatchesat50percent decimal(38, 4),
        videowatchesat75percent decimal(38, 4),
        videowatchesat95percent decimal(38, 4),
        videowatchesat100percent decimal(38, 4),
        videoaveragewatchtime decimal(38, 4),
        clickstoplayvideo decimal(38, 4),
        uniqueclickstoplayvideo decimal(38, 4),
        avgcanvasviewtimeseconds decimal(38, 4),
        avgcanvasviewpercentage string,
        costperthreesecondvideoview decimal(38, 4),
        websiteconversions decimal(38, 4),
        websiteconversionrate string,
        websitecustomconversions decimal(38, 4),
        websiteleads decimal(38, 4),
        websitepurchases decimal(38, 4),
        costperwebsiteconversion decimal(38, 4),
        costperwebsitecustomconversions decimal(38, 4),
        costperwebsitelead decimal(38, 4),
        costperwebsitepurchase decimal(38, 4),
        videoplayactions decimal(38, 4),
        frequency decimal(38, 4),
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
            year(date) as year,
            year(date) || '|' || month(date) as yearmonth,
            month(date) as month,
            day(date) as dayofmonth,
            dayofweek(date) || ' ' || date_format(date, 'EEEE') as dayofweek,
            profile as account,
            profileid as accountid,
            date,
            adcampaign_name as campaignname,
            adcampaign_id as campaignid,
            campaignstatus,
            ifnull(campaign_daily_budget::decimal(38, 4), 0) as campaigndailybudget,
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
            ifnull(cost::decimal(38, 4), 0) as cost,
            ifnull(reach::decimal(38, 4), 0) as reach,
            ifnull(impressions::decimal(38, 4), 0) as impressions,
            ifnull(cpm::decimal(38, 4), 0) as cpmcostper1000impressions,
            ifnull(action_link_click::decimal(38, 4), 0) as linkclicks,
            ifnull(unique_action_link_click::decimal(38, 4), 0) as uniquelinkclicks,
            link_ctr as ctrlinkclickthroughrate,
            ifnull(cplc::decimal(38, 4), 0) as cpccostperlinkclick,
            ifnull(ucplc::decimal(38, 4), 0) as costperuniquelinkclick,
            ifnull(action_video_view::decimal(38, 4), 0) as threesecondvideoviews,
            ifnull(unique_action_video_view::decimal(38, 4), 0) as uniquethreesecondvideoviews,
            ifnull(video_30_sec_watched_actions::decimal(38, 4), 0) as thirtysecondvideoviews,
            ifnull(video_p25_watched_actions::decimal(38, 4), 0) as videowatchesat25percent,
            ifnull(video_p50_watched_actions::decimal(38, 4), 0) as videowatchesat50percent,
            ifnull(video_p75_watched_actions::decimal(38, 4), 0) as videowatchesat75percent,
            ifnull(video_p95_watched_actions::decimal(38, 4), 0) as videowatchesat95percent,
            ifnull(video_p100_watched_actions::decimal(38, 4), 0) as videowatchesat100percent,
            ifnull(video_average_watch_time::decimal(38, 4), 0) as videoaveragewatchtime,
            ifnull(action_video_play::decimal(38, 4), 0) as clickstoplayvideo,
            ifnull(unique_action_video_play::decimal(38, 4), 0) as uniqueclickstoplayvideo,
            ifnull(canvas_avg_view_time::decimal(38, 4), 0) as avgcanvasviewtimeseconds,
            canvas_avg_view_percent as avgcanvasviewpercentage,
            ifnull(cost_per_3s_video_view::decimal(38, 4), 0) as costperthreesecondvideoview,
            ifnull(offsite_conversions::decimal(38, 4), 0) as websiteconversions,
            offsite_conversion_rate as websiteconversionrate,
            ifnull(offsite_conversions_fb_pixel_custom::decimal(38, 4), 0) as websitecustomconversions,
            ifnull(offsite_conversions_fb_pixel_lead::decimal(38, 4), 0) as websiteleads,
            ifnull(offsite_conversions_fb_pixel_purchase::decimal(38, 4), 0) as websitepurchases,
            ifnull(cost_per_website_conversion::decimal(38, 4), 0) as costperwebsiteconversion,
            ifnull(cost_per_website_custom_conversion::decimal(38, 4), 0) as costperwebsitecustomconversions,
            ifnull(cost_per_website_lead::decimal(38, 4), 0) as costperwebsitelead,
            ifnull(cost_per_website_purchase::decimal(38, 4), 0) as costperwebsitepurchase,
            ifnull(video_play_actions::decimal(38, 4), 0) as videoplayactions,
            ifnull(frequency::decimal(38, 4), 0) as frequency,
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
