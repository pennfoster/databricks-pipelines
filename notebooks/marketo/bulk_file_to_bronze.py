# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths

# COMMAND -----
raw_path = get_mount_paths("marketo").landing

for table_directory in dbutils.fs.ls(raw_path):
    csv_files = [
        file.path
        for file in dbutils.fs.ls(table_directory.path)
        if file.path.endswith(".csv")
    ]
    jsonl_files = [
        file.path
        for file in dbutils.fs.ls(table_directory.path)
        if file.path.endswith(".jsonl")
    ]

    if not (jsonl_files or csv_files):
        continue
    if jsonl_files and csv_files:
        raise TypeError("Mixed file types")
    if csv_files:
        df = spark.read.csv(
            path=csv_files,
            header=True,
            escape='"',
        )
    if jsonl_files:
        df = spark.read.json(path=jsonl_files)

    bronze_dest = f"{get_mount_paths('marketo').bronze}/{table_directory.name}"
    df.write.format("delta").mode("append").option("mergeSchema", True).option(
        "overwriteSchema",
        True,
    ).save(bronze_dest)

    processed_dest = f"{table_directory.path}/processed"
    dbutils.fs.mkdirs(processed_dest)
    for file in [*csv_files, *jsonl_files]:
        dbutils.fs.mv(file, processed_dest)

    print(f"{table_directory.name} complete")
