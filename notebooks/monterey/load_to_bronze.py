# Databricks notebook source
from pyspark.sql.utils import AnalysisException


from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import (
    add_basic_metadata,
    add_version_flags,
)

DATA_SOURCE = "monterey"
root_raw_path = get_mount_paths(DATA_SOURCE).landing

# COMMAND -----
table_variables = {
    "Declines": {"internal_date_column": "PaymentEffectiveDate"},
    "Transactions": {"internal_date_column": "PaymentEffectiveDate"},
}

# COMMAND -----
for table_name, columns in table_variables.items():
    raw_source_path = f"{root_raw_path}/{table_name}"
    bronze_dest_path = f"{get_mount_paths(DATA_SOURCE).bronze}/{table_name}"

    csv_files = sorted(
        [f.path for f in dbutils.fs.ls(raw_source_path) if f.path.endswith(".csv")]
    )
    if not csv_files:
        continue

    # Raw data with load metadata
    for n, file in enumerate(csv_files):
        if n == 0:
            raw_df = add_basic_metadata(spark.read.csv(path=file, header=True))
            continue
        sub_df = add_basic_metadata(spark.read.csv(path=file, header=True))
        raw_df = raw_df.unionByName(sub_df, allowMissingColumns=True)

    # Union existing with new
    try:
        bronze_table = spark.read.load(bronze_dest_path)
        dirty_df = bronze_table.unionByName(raw_df, allowMissingColumns=True)
    except AnalysisException as e:
        if any([s in e.desc for s in ["Path does not exist", "is not a Delta table"]]):
            dirty_df = raw_df
        else:
            raise e

    versioned_df = add_version_flags(
        df=dirty_df,
        partition_by_col=columns["internal_date_column"],
    )

    #! Is overwriting here correct? Should table be edited in place instead?
    versioned_df.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(bronze_dest_path)

    processed_path = f"{raw_source_path}/processed"
    dbutils.fs.mkdirs(processed_path)
    for file in csv_files:
        dbutils.fs.mv(file, processed_path)

    print(f"{table_name} complete")
