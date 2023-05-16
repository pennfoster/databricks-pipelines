insert into
    silver_supermetrics.bingcampaign
select
    'AWBingCampaign' as queryname,
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
FROM
    bronze_supermetrics.bingcampaign_awbingcampaign;

"""
spark.sql(create_query)



-- queryname varchar(250)
-- , date date
-- , year varchar(255)
-- , yearmonth varchar(255)
-- , month varchar(255)
-- , dayofmonth varchar(255)
-- , dayofweek varchar(255)
-- , campaignname varchar(255)
-- , campaignid varchar(255)
-- , accountname varchar(255)
-- , accountid varchar(255)
-- , accountnumber varchar(255)
-- , status varchar(255)
-- , campaigntype varchar(255)
-- , campaignlabels varchar(255)
-- , impressions varchar(255)
-- , clicks float
-- , cost float
-- , ctrpercentage float
-- , cpc float
-- , cpm float
-- , averageposition float
-- , conversions float
-- , conversionrate varchar(255)
-- , conversionsperimpressionrate varchar(255)
-- , CostPerConversion float
-- , RevenuePerConversion float
-- , revenue float
-- , returnonadspend float
-- , campaignvertical varchar(255)
-- , record_insert_date timestamp
-- )