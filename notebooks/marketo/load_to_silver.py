# Databricks notebook source
from pyspark.sql import Window
from shared.functions.azure_utilities import get_mount_paths


DATA_SOURCE = "monterey"
bronze_path = get_mount_paths(DATA_SOURCE).bronze
silver_path = get_mount_paths(DATA_SOURCE).silver

# COMMAND ----------
table_variables = {
    "activities/": {
        "uid": "marketoguid",
        "date_field": "activitydate"
        # json activity column. unpack?
    },
    "channels/": {"uid": "id", "date_field": "updatedAt"},
    "email_templates/": {"uid": "id", "date_field": "updatedAt"},
    "emails/": {"uid": "id", "date_field": "updatedAt"},
    "form_fields/": {"uid": "id", "date_field": "updatedAt"},
    "forms/": {"uid": "id", "date_field": "updatedAt"},
    "landing_pages/": {"uid": "id", "date_field": "updatedAt"},
    "leads/": {
        "uid": "id",
        "date_field": "updatedAt",
        "where_clause": "try_cast(id as integer) is not null",
    },
    "snippets/": {"uid": "id", "date_field": "updatedAt"},
}
# COMMAND ----------
for table in dbutils.fs.ls(bronze_path):
    bronze_df = spark.read.load(path=table.path, format="delta")

    uid = table_variables[table.name]["uid"]
    date_field = table_variables[table.name]["date_field"]
    where_clause = table_variables[table.name].get("where_clause")

    current_data = bronze_df.filter(
        (bronze_df["_most_recent_data_for_date"] == True)
        & (bronze_df["_deleted_at_source"] == False)
    )

    if where_clause:
        current_data = current_data.where(where_clause)

    current_data.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(f"{silver_path}/{table.name}")

    print(f"{table.name} complete.")
