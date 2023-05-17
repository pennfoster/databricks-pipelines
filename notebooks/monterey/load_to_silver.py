# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

DATA_SOURCE = "monterey"
bronze_path = get_mount_paths(DATA_SOURCE).bronze
silver_path = get_mount_paths(DATA_SOURCE).silver

# COMMAND ----------
for table_fs in dbutils.fs.ls(bronze_path):
    bronze_df = spark.read.load(path=table_fs.path, format="delta")

    current_data = bronze_df.filter((bronze_df["_latest"] == True))

    current_data.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(f"{silver_path}/{table_fs.name}")

    print(f"{table_fs.name} complete.")
