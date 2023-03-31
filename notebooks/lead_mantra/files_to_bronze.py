# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths
from pathlib import Path
import logging

# COMMAND -----
DATA_SOURCE = "lead_mantra"
root_path = get_mount_paths(DATA_SOURCE).landing
# COMMAND -----
for table_directory in Path(f"/dbfs/{root_path}").iterdir():
    if table_directory.is_file():
        logging.warning("File in root directory")
        continue
    print(f"processing {table_directory.name}...")
    jsonl_files = [
        str(file).replace("/dbfs/", "")
        for file in table_directory.iterdir()
        if file.name.endswith(".jsonl")
    ]

    if not jsonl_files:
        continue

    df = spark.read.json(jsonl_files)

    bronze_dest = f"{get_mount_paths(DATA_SOURCE).bronze}/{table_directory.name}"
    df.write.format("delta").mode("append").option("mergeSchema", True).option(
        "overwriteSchema",
        True,
    ).save(bronze_dest)

    processed_dest = f"{str(table_directory).replace('/dbfs/','')}/processed"
    dbutils.fs.mkdirs(processed_dest)
    for file in jsonl_files:
        dbutils.fs.mv(file, processed_dest)

    print(f"{table_directory.name} complete")
