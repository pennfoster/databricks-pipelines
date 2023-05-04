# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

# COMMAND -----
DATA_SOURCE = "monterey"
raw_path = get_mount_paths(DATA_SOURCE).landing

for table_directory in dbutils.fs.ls(raw_path):
    csv_files = [
        file.path
        for file in dbutils.fs.ls(table_directory.path)
        if file.path.endswith(".csv")
    ]

    if not csv_files:
        continue
    if csv_files:
        df = spark.read.csv(
            path=csv_files,
            header=True,
        )

    from pyspark.sql import DataFrame
    from pyspark.sql.functions import current_timestamp

    from shared.functions.github_utilities import get_current_repo_branch

    def insert_bronze_metadata_columns(df: DataFrame):
        df = df.withColumns(
            {
                "_bronze_insert_ts": current_timestamp(),
                "_repo_branch": get_current_repo_branch(),
            }
        )

    bronze_dest = f"{get_mount_paths(DATA_SOURCE).bronze}/{table_directory.name}"
    df.write.format("delta").mode("append").option("mergeSchema", True).option(
        "overwriteSchema",
        True,
    ).save(bronze_dest)

    processed_dest = f"{table_directory.path}/processed"
    dbutils.fs.mkdirs(processed_dest)
    for file in csv_files:
        dbutils.fs.mv(file, processed_dest)

    print(f"{table_directory.name} complete")
