# Databricks notebook source
from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import (
    add_insert_data,
    add_data_version_flags,
)

DATA_SOURCE = "monterey"
root_raw_path = get_mount_paths(DATA_SOURCE).landing

# COMMAND -----
table_variables = {
    "Declines": {
        "uid_list": [
            "ClassAccount",
            "ContractID",
            "AcctNumber",
        ],
        "intertnal_date_column": "PaymentEffectiveDate",
    },
    "Transactions": {
        "uid_list": [
            "ClassAccount",
            "ContractID",
            "AcctNumber",
        ],
        "intertnal_date_column": "PaymentEffectiveDate",
    },
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
            raw_df = add_insert_data(spark.read.csv(path=file, header=True))
            continue
        sub_df = add_insert_data(spark.read.csv(path=file, header=True))
        raw_df.unionByName(sub_df, allowMissingColumns=True)

    # Union existing with new
    dirty_bronze_df = spark.read.load(bronze_dest_path, "delta").unionByName(
        raw_df, allowMissingColumns=True
    )

    # ? df_w_insert_data.write.format("delta").mode("append").option(
    # ?     "mergeSchema", True
    # ? ).option("overwriteSchema", True,).save(bronze_dest_path)

    versioned_df = add_data_version_flags(
        df=dirty_bronze_df,
        internal_date_col=columns["intertnal_date_column"],
        metadata_date_col="_bronze_insert_ts",
    )

    # remove unecessary duplicates
    cleaned_bronze_df = dirty_bronze_df.filter(
        (dirty_bronze_df["_initial_data_for_date"] == True)
        | (dirty_bronze_df["_most_recent_data_for_date"] == True)
        | (dirty_bronze_df["_deleted_at_source"] == True)
    )

    #! Is overwriting here correct? Should table be edited in place instead?
    cleaned_bronze_df.write.format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).option("overwriteSchema", True,).save(bronze_dest_path)

    # processed_path = f"{raw_source_path}/processed"
    # dbutils.fs.mkdirs(processed_path)
    # for file in csv_files:
    #     dbutils.fs.mv(file, processed_path)

    print(f"{table_name} complete")
