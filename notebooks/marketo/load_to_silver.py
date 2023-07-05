# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

DATA_SOURCE = "marketo"

bronze_path = get_mount_paths(DATA_SOURCE).bronze
silver_path = get_mount_paths(DATA_SOURCE).silver

table_variables = {
    "leads/": {
        "where_clause": "try_cast(id as integer) is not null",
    }
}
# COMMAND ----------
for table in dbutils.fs.ls(bronze_path):
    bronze_df = spark.read.load(path=table.path, format="delta")

    current_data = bronze_df.filter(bronze_df["_latest"] == True)

    if table.name in table_variables.keys():
        where_clause = table_variables[table.name]["where_clause"]
        current_data = current_data.where(where_clause)

    current_data.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(f"{silver_path}/{table.name}")

    print(f"{table.name} complete.")
