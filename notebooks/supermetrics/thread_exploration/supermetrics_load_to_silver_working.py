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

# COMMAND -----
tz = timezone(COMPANY_TIMEZONE)
seven_days_ago = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")

dbutils.widgets.text("end_date",datetime.now(tz).strftime("%Y-%m-%d"), "")
dbutils.widgets.combobox("start_date", seven_days_ago, [seven_days_ago, "1900-01-01"])

url_df = get_url_dataframe()
search_names = url_df["C001_SearchName"].sort_values().unique().tolist()
search_names += [""]

dbutils.widgets.dropdown("search_name", "", search_names)

# COMMAND -----
search_name = dbutils.widgets.get("search_name")
if not search_name:
   raise ValueError('Empty search_name parameter provided.')
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

where_clause_start = f"where date >= '{start_date}'"
if end_date:
    where_clause_end = f"and date <= '{end_date}'"


data_source = 'supermetrics'
silver_db = f'silver_{data_source}'
bronze_db = f'bronze_{data_source}'
paths = get_mount_paths(data_source)
bronze_dir = f"{paths.bronze}/{search_name}/"
silver_dir = f"{paths.silver}/{search_name}"


# COMMAND -----
# indx = 1
# row = df.iloc[1, :]
# search_name = row["C001_SearchName"]
# bronze_path = f"mnt/bronze/supermetrics/{search_name}"
paths = [i for i in Path("/dbfs" + bronze_dir).iterdir() if i.is_dir()]
queries = [i.parts[-1] for i in paths]
delta_dirs = [i.joinpath(j) for i, j in zip(paths, queries)]

# COMMAND -----
# Prep
# spark.sql(f'create database if not exists {silver_db}')
# Path(f"/dbfs/{silver_dir}").mkdir(parents=True, exist_ok=True)
# ts = TableSchemas(data_source=data_source, table_name=search_name)
# create_clause = ts.get_create_clause()
# query = f'''
#     create or replace table {silver_db}.{search_name} (
#       queryname varchar(500)
#       , year int
#       , yearmonth varchar(500)
#       , month int
#       , dayofmonth int
#       , dayofweek varchar(500)
#       , {create_clause}
#     )
#     using delta
#     partitioned by (queryname)
#     location '{silver_dir}'
# '''
# spark.sql(query)

# COMMAND -----
# TESTING
# TODO: Remove testing cell
# a = delta_dirs[0]
query_name = a.name
table_dir = '/' / Path(*a.parts[2:])
bronze_table_path = f'bronze_{data_source}.{search_name.lower()}_{query_name.lower()}'
# df = spark.read.format('delta') \
#     .option("startingVersion", "latest") \
#     .option("query", "select * limit 1") \
#     .table(bronze_table_path)
history = spark.sql(f'describe history {bronze_db}.{search_name}_{query_name}').toPandas()
latest_version = history['version'].max()
select_clause = ts.get_select_clause(nullif='')
query = f'''
    select
      '{query_name}' as queryname
      , year(date) as year
      , year(date) || '|' || month(date) as yearmonth
      , month(date) as month
      , day(date) as dayofmonth
      , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
      , {select_clause}
    from
      {bronze_db}.{search_name}_{query_name}@v{latest_version}
'''
    # --{where_clause_start} -- these are for filtering dates to not rebuild table each time
    # --{where_clause_end} -- this is for filtering dates to not rebuild table each time
spdf = spark.sql(query)

# pddf = spdf.toPandas()
# dates = pddf.date.unique().tolist()
# spark.sql(f'''
#     delete from {silver_db}.{search_name} 
#     where dates in ({dates}) 
#     and lower(queryname) = lower({query_name})
# ''')
          
spdf.write.format("delta").mode("append").option(
    "mergeSchema", True
).option("overwriteSchema", True).option("replaceWhere", f"queryname = '{query_name}'").partitionBy("queryname").save(f"{silver_dir}")

spark.sql(f'''
    create table if not exists {silver_db}.{search_name} location '{silver_dir}'
''')
# spark.sql(f'''
#     create table if not exists {silver_db}.{search_name} location '{silver_db}.{search_name}'
# ''')
# COMMAND -----
# spark.sql(f'''
#    merge into {silver_db}.{search_name} dest
#    using ({query}) src
#    on dest.date = src.date
#    when matched delete
# ''')
# COMMAND -----

# spark.read.format("delta") \
#   .option("readChangeFeed", "true") \
#   .option("startingVersion", 0) \
#   .table("myDeltaTable")


#   .option("query", "SELECT * FROM mytable WHERE column1 > 100")


# dir = paths[0]
# dir_mnt = Path(*dir.parts[2:])
# COMMAND -----
base_columns = '''
    year(date) as year
   , year(date) || '|' || month(date) as yearmonth
   , month(date) as month
   , day(date) as dayofmonth
   , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
'''
# select
#   replace(customurlparameters, '"', '') as customurlparameters
#   , year(date) as year
#   , year(date) || '|' || month(date) as yearmonth
#   , month(date) as month
#   , day(date) as dayofmonth
#   , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
#   , customurlparameters:mkwid as mkwid
#   , customurlparameters:dskeyname as mkwid
#   , customurlparameters:pubcode as pubcode
#   , customurlparameters:adkey as adkey
#   , customurlparameters:dskeyword as dskeyword
#   , * except(customurlparameters)




spark.sql(
    f"""
    select * 
    from delta.`{'/' / Path(*d.parts[2:])}`
    
"""
spark.read.option("startingVersion", "latest").load(c.as_posix()).display()
)

  spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_{SOURCE}_{GROUP}.table_column_mapping (bronze_table STRING, bronze_column STRING, silver_table STRING, silver_column STRING, primary_key BOOLEAN, convert_julian_date BOOLEAN) PARTITIONED BY (bronze_table) LOCATION '/mnt/sadatalakehouse{ENV}001_bronze/{SOURCE}/{GROUP}/table_column_mapping'")
# %sql
# select
#   replace(customurlparameters, '"', '') as customurlparameters
#   , year(date) as year
#   , year(date) || '|' || month(date) as yearmonth
#   , month(date) as month
#   , day(date) as dayofmonth
#   , day(date) || ' ' || date_format(date, 'EEEE') as dayofweek
#   , customurlparameters:mkwid as mkwid
#   , customurlparameters:dskeyname as mkwid
#   , customurlparameters:pubcode as pubcode
#   , customurlparameters:adkey as adkey
#   , customurlparameters:dskeyword as dskeyword
#   , * except(customurlparameters)
# from
#   bronze_supermetrics.testing_testingfirsttest
# order by
#   2


Path(f"/dbfs/{silver_dir}").mkdir(parents=True, exist_ok=True)
s = ts.get_create_clause()
q = f'''
    create table if not exists {silver_db}.{search_name} (
      queryname varchar(500)
      , year int
      , yearmonth varchar(500)
      , month int
      , dayofmonth int
      , dayofweek varchar(500)
      , {s}
    )
    using delta
    location '{silver_dir}'
'''
spark.sql(q)
