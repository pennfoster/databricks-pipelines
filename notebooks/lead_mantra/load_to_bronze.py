# Databricks notebook source
from pyspark.sql.utils import AnalysisException

from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import (
    add_version_flags,
    add_basic_metadata,
)

mnt_path = get_mount_paths("lead_mantra")
failures = {}

# COMMAND -----
table_variables = {
    "campaigns/": {"uid": "campaign_id"},
    "lead_status/": {"uid": "external_system_id"},
    "leads/": {"uid": "external_system_id"},
    "message_attempt/": {"uid": "external_system_id"},
    "message_log/": {"uid": "external_system_id"},
}

# COMMAND -----
for table in dbutils.fs.ls(mnt_path.landing):
    print(f"processing {table.name}...")
    bronze_table_path = f"{mnt_path.bronze}/{table.name}"

    jsonl_files = [
        file.path for file in dbutils.fs.ls(table.path) if file.name.endswith(".jsonl")
    ]
    if not jsonl_files:
        continue

    # Files are read into DataFrames individually to assure unique insert timestamps
    for n, file in enumerate(sorted(jsonl_files)):
        df = spark.read.json(path=jsonl_files)
        file_df = add_basic_metadata(df)

        if n == 0:
            raw_df = file_df
            continue
        raw_df = raw_df.unionByName(file_df, allowMissingColumns=True)

    # Union new data with existing bronze
    try:
        bronze_table = spark.read.load(bronze_table_path)
        dirty_df = bronze_table.unionByName(raw_df, allowMissingColumns=True)
    except AnalysisException as e:
        if "is not a Delta table" in e.desc:
            dirty_df = raw_df
        else:
            raise e

    # Update versioning flags
    uid_column = table_variables[table.name]["uid"]
    versioned_df = add_version_flags(
        df=dirty_df,
        partition_by_col=uid_column,
        meta_ingestion_date_col="_bronze_insert_ts",
    )

    # Write to bronze
    versioned_df.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(bronze_table_path)

    # Move processed files
    processed_dest = f"{table.path}/processed"
    dbutils.fs.mkdirs(processed_dest)
    for file in jsonl_files:
        dbutils.fs.mv(file, processed_dest)

    print(f"{table.name} complete")
