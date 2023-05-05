# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

# COMMAND ----------

mnt = get_mount_paths("monterey")

for table_name in mnt.bronze:
    bronze_path = f"{mnt.bronze}/{table_name}"
    silver_path = f"{mnt.silver}/{table_name}"

    bronze_table = spark.read.load(path=bronze_path, format="delta")
# COMMAND ----------
#   select distinct
#      *
#   from bronze
#   excluding fp, branch
#   where ts = max(ts) over (partition by non-metadata)
#


# display(df.distinct().orderBy(["ClassAccount", "ContractID", "PaymentEffectiveDate"]))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   contractid,
# MAGIC   classaccount,
# MAGIC   acctnumber,
# MAGIC   paymentEffectivedate,
# MAGIC   count(*) as c
# MAGIC FROM
# MAGIC   (select distinct *
# MAGIC   from delta.`/mnt/sadatalakehousedev001_bronze/monterey/Declines`)
# MAGIC group by 1,2,3,4
# MAGIC having c >1
# MAGIC ;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select distinct
# MAGIC   *
# MAGIC FROM delta.`/mnt/sadatalakehousedev001_bronze/monterey/Declines`
# MAGIC where contractid=0933160
# MAGIC and classaccount=503087010
# MAGIC and acctnumber =8767
# MAGIC and paymenteffectivedate='2020-05-01'
