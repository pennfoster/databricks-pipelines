# Databricks notebook source
from pyspark.sql.utils import AnalysisException

from shared.functions.azure_utilities import get_mount_paths
from shared.functions.metadata_utilities import add_data_version_flags, add_insert_data

mnt_path = get_mount_paths("marketo")

failures = {}
# COMMAND -----
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
    "leads/": {"uid": "id", "date_field": "updatedAt"},
    "snippets/": {"uid": "id", "date_field": "updatedAt"},
    # "programs/": {"uid": None, "date_field": None},  # empty
    # "smart_campaigns/": {"uid": None, "date_field": None},  # empty
    # "smart_lists/": {"uid": None, "date_field": None},  # empty
    # "static_lists/": {"uid": None, "date_field": None},  # empty
}

# COMMAND -----
for table in dbutils.fs.ls(mnt_path.landing):
    bronze_table_path = f"{mnt_path.bronze}/{table.name}"

    try:
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

        for n, file in enumerate([*csv_files, *jsonl_files]):
            if csv_files:
                df = spark.read.csv(
                    path=csv_files,
                    header=True,
                    escape='"',
                )
            if jsonl_files:
                df = spark.read.json(path=jsonl_files)

            i_df = add_insert_data(df)

            if n == 0:
                raw_df = i_df
                continue
            raw_df = raw_df.unionByName(i_df, allowMissingColumns=True)

        try:
            bronze_table = spark.read.load(f"{mnt_path.bronze}/{table.name}")
            dirty_df = bronze_table.unionByName(raw_df, allowMissingColumns=True)
        except AnalysisException as e:
            if not ("is not a Delta table" in e.desc):
                raise e
            dirty_df = raw_df

        versioned_df = add_data_version_flags(
            df=dirty_df,
            internal_date_col=table_variables[table.name]["date_field"],
            metadata_date_col="_bronze_insert_ts",
        )

        clean_df = versioned_df.filter(
            (versioned_df["_initial_data_for_date"] == True)
            | (versioned_df["_most_recent_data_for_date"] == True)
            | (versioned_df["_deleted_at_source"] == True)
        )

        df.write.format("delta").mode("overwrite").option("mergeSchema", True).option(
            "overwriteSchema",
            True,
        ).save(bronze_table_path)

        processed_dest = f"{table.path}/processed"
        dbutils.fs.mkdirs(processed_dest)
        for file in [*csv_files, *jsonl_files]:
            dbutils.fs.mv(file, processed_dest)

        print(f"{table.name} complete")

    except Exception as e:
        failures[table.name] = e.__repr__()
        continue

# COMMAND ----------
if any(failures):
    import json

    raise Exception(json.dumps(failures))
