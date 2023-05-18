# Databricks notebook source
# DBTITLE 1, Imports and Variables
import logging

from pyspark.sql.utils import AnalysisException

from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import (
    add_basic_metadata,
    add_version_flags,
)

mnt_path = get_mount_paths("marketo")

failures = {}
table_variables = {
    "activities/": {
        "uid": "marketoguid",
        # json activity column. unpack?
    },
    "channels/": {"uid": "id"},
    "email_templates/": {"uid": "id"},
    "emails/": {"uid": "id"},
    "form_fields/": {"uid": "id"},
    "forms/": {"uid": "id"},
    "landing_pages/": {"uid": "id"},
    "leads/": {"uid": "id"},
    "snippets/": {"uid": "id"},
    "programs/": {"uid": "id"},
    "smart_campaigns/": {"uid": "id"},
    "smart_lists/": {"uid": "id"},
    "static_lists/": {"uid": "id"},
}

# COMMAND -----
# DBTITLE 1, Load Raw to Bronze
for table in dbutils.fs.ls(mnt_path.landing):
    bronze_table_path = f"{mnt_path.bronze}/{table.name}"

    try:
        # check for new files
        csv_files = [
            file.path
            for file in dbutils.fs.ls(table.path)
            if file.path.endswith(".csv")
        ]
        jsonl_files = [
            file.path
            for file in dbutils.fs.ls(table.path)
            if file.path.endswith(".jsonl")
        ]

        if not (jsonl_files or csv_files):
            continue
        if jsonl_files and csv_files:
            raise TypeError("Mixed file types")

        # Files are read into DataFrames individually to assure unique insert timestamps
        for n, file in enumerate(sorted([*csv_files, *jsonl_files])):
            if csv_files:
                df = spark.read.csv(
                    path=csv_files,
                    header=True,
                    escape='"',
                )
            if jsonl_files:
                df = spark.read.json(path=jsonl_files)

            file_df = add_basic_metadata(df)
            if n == 0:
                raw_df = file_df
                continue
            raw_df = raw_df.unionByName(file_df, allowMissingColumns=True)

        # Union new data with existing bronze
        try:
            bronze_table = spark.read.load(f"{mnt_path.bronze}/{table.name}")
            dirty_df = bronze_table.unionByName(raw_df, allowMissingColumns=True)
        except AnalysisException as e:
            if any(
                [s in e.desc for s in ["Path does not exist", "is not a Delta table"]]
            ):
                raise e
            dirty_df = raw_df

        # Update versioning flags
        versioned_df = add_version_flags(
            df=dirty_df,
            partition_by_col=table_variables[table.name]["uid"],
        )

        # Write to bronze
        versioned_df.write.format("delta").mode("overwrite").option(
            "mergeSchema", True
        ).option("overwriteSchema", True,).save(bronze_table_path)

        # Move processed files
        processed_dest = f"{table.path}/processed"
        dbutils.fs.mkdirs(processed_dest)
        for file in [*csv_files, *jsonl_files]:
            dbutils.fs.mv(file, processed_dest)

        print(f"{table.name} complete")

    except Exception as e:
        logging.warning("Failure in %s" % table.name)
        logging.warning(e.__reduce__())
        failures[table.name] = e.__repr__()
        continue

# COMMAND ----------
# DBTITLE 1, Check for failures
if any(failures):
    import json

    print(json.dumps(failures, indent=4))
    raise Exception
