import requests

import pendulum
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    input_file_name,
    lit,
    when,
    min,
    max,
    hash,
)

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


def add_insert_data(df: DataFrame):
    return df.withColumns(
        {
            "_bronze_insert_ts": lit(pendulum.now()),
            "_raw_file_source": input_file_name(),
            "_code_version": lit(get_current_repo_branch()),
        }
    )


def add_data_version_flags(
    df: DataFrame,
    partition_by_col: str,
    meta_ingestion_date_col: str = "_bronze_insert_ts",
) -> DataFrame:
    original_columns = [c for c in df.columns if not c.startswith("_")]

    # TODO: add hash column first
    hash_df = df.withColumn(
        "_row_hash",
        hash(*original_columns),
    )

    hash_partition = Window().partitionBy("_row_hash")
    primary_partition = Window().partitionBy(partition_by_col)

    versioned_df = hash_df.withColumns(
        {
            "_initial": when(
                (
                    hash_df[meta_ingestion_date_col]
                    == min(meta_ingestion_date_col).over(primary_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
            "_latest": when(
                (
                    hash_df[meta_ingestion_date_col]
                    == max(meta_ingestion_date_col).over(primary_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
            "_update": when(
                (
                    hash_df[meta_ingestion_date_col]
                    != min(meta_ingestion_date_col).over(primary_partition)
                )
                & (
                    hash_df[meta_ingestion_date_col]
                    == min(meta_ingestion_date_col).over(hash_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
        }
    )

    return versioned_df


def add_deleted_transaction_flag(
    input_df: DataFrame,
    transaction_date_col: str,
    meta_ingestion_date_col: str = "_bronze_insert_ts",
):
    date_partition = Window().partitionBy(transaction_date_col)

    latest_df = (
        input_df.select(input_df["_hash"].alias("latest_hash"))
        .filter(input_df["_latest"] == True)
        .filter(
            input_df[meta_ingestion_date_col]
            == max(meta_ingestion_date_col).over(date_partition)
        )
        .distinct()
    )

    join_df = input_df.join(
        latest_df, on=input_df["_hash"] == latest_df["latest_hash"], how="fullouter"
    )

    output_df = join_df.withColumn("_deleted", when((join_df["latest_hash"].isNull())))

    return output_df


# def add_data_version_flags(
#     df: DataFrame,
#     internal_date_col: str,
#     metadata_date_col: str,
# ) -> DataFrame:
#     """Adds 3 metadata columns to spark DataFrame:
#         (
#             "_initial_data_for_date",
#             "_most_recent_data_for_date",
#             "_deleted_at_source"
#         )
#     This function *requires* that the DataFrame's metadata column names start with an underscore ("_")

#     Args:
#         df (DataFrame): _description_
#         internal_date_col (Union[Column, str]): _description_
#         metadata_date_col (Union[Column, str]): _description_

#     Returns:
#         _type_: _description_
#     """
#     original_columns = [c for c in df.columns if not c.startswith("_")]

#     current_window = Window().partitionBy(internal_date_col)
#     outdated_window = Window().partitionBy(original_columns)

#     output_df = df.withColumns(
#         {
#             "_initial_data_for_date": when(
#                 (df[metadata_date_col] == min(metadata_date_col).over(current_window)),
#                 True,
#             ).otherwise(
#                 False,
#             ),
#             "_most_recent_data_for_date": when(
#                 (df[metadata_date_col] == max(metadata_date_col).over(current_window)),
#                 True,
#             ).otherwise(
#                 False,
#             ),
#             "_deleted_at_source": when(
#                 (df[metadata_date_col] != max(metadata_date_col).over(current_window))
#                 & (
#                     df[metadata_date_col]
#                     == max(metadata_date_col).over(outdated_window)
#                 ),
#                 True,
#             ).otherwise(
#                 False,
#             ),
#         }
#     )

#     return output_df


def get_current_repo_branch():
    filepath = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )

    repo_path = filepath.rsplit("/", 3)[0]
    url = f"https://{sc.getConf().get('spark.databricks.workspaceUrl')}/api/2.0/repos"
    access_token = dbutils.secrets.get(get_key_vault_scope(), "cicd-access-token")
    response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    response.raise_for_status()
    for repo in response.json()["repos"]:
        if repo["path"].startswith(repo_path):
            return repo["branch"]
