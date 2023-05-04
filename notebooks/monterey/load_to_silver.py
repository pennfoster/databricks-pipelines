# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

# COMMAND ----------

mnt = get_mount_paths("monterey")

# COMMAND ----------

table = "Declines"
bronze_table = f"{mnt.bronze}/Declines"
print(bronze_table)

# COMMAND ----------

df = spark.read.format("delta").load(bronze_table)
df.distinct().count()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.distinct().orderBy(["ClassAccount", "ContractID", "PaymentEffectiveDate"]))


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
