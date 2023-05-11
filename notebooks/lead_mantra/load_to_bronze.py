# Databricks notebook source
from pyspark.sql.utils import AnalysisException

from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import add_data_version_flags, add_insert_data

mnt_path = get_mount_paths("lead_mantra")

failures = {}
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
        df_plus = add_insert_data(df)

        if n == 0:
            raw_df = df_plus
            continue
        raw_df = raw_df.unionByName(df_plus, allowMissingColumns=True)

    # Union new data with existing bronze
    try:
        bronze_table = spark.read.load(bronze_table_path)
        dirty_df = bronze_table.unionByName(raw_df, allowMissingColumns=True)
    except AnalysisException as e:
        if not ("is not a Delta table" in e.desc):
            raise e
        dirty_df = raw_df

    # Update versioning flags
    versioned_df = add_data_version_flags(
        df=dirty_df,
        internal_date_col=table_variables[table.name]["date_field"],
        metadata_date_col="_bronze_insert_ts",
    )

    # Drop duplicates
    clean_df = versioned_df.filter(
        (versioned_df["_initial_data_for_date"] == True)
        | (versioned_df["_most_recent_data_for_date"] == True)
        | (versioned_df["_deleted_at_source"] == True)
    )

    # Write to bronze
    clean_df.write.format("delta").mode("overwrite").option("mergeSchema", True).option(
        "overwriteSchema",
        True,
    ).save(bronze_table_path)

    # Move processed files
    processed_dest = f"{table.path}/processed"
    dbutils.fs.mkdirs(processed_dest)
    for file in jsonl_files:
        dbutils.fs.mv(file, processed_dest)

    print(f"{table.name} complete")
