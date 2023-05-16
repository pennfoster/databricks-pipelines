import requests

import pendulum
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import input_file_name, lit, when, min, max, concat_ws, sha2

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


def add_basic_metadata(df: DataFrame, filename_override: str = None):
    """Adds a series of metadata columns to a DataFrame, includeing:
        \b
        _row_hash: A sha256 hash of non-metadata column values to allow for future row comparisons.
        _bronze_insert_ts: UTC timestamp at the momement of ingestion,
        _bronze_update_ts: As above (intended to future proof the table in case an update date becomes necessary),
        _code_version: The git branch the repo running the code is on at the moment of ingestion,
        _raw_file_source: The original file source. Requires an override if using a Pandas DataFrame

    Returns:
        DataFrame
    """
    sorted_columns = sorted([c for c in df.columns if not c.startswith("_")])

    return df.withColumns(
        {
            "_row_hash": lit(sha2(concat_ws("|", *sorted_columns), 256)),
            "_bronze_insert_ts": lit(pendulum.now()),
            "_bronze_update_ts": lit(pendulum.now()),
            "_code_version": lit(get_current_repo_branch()),
            "_raw_file_source": lit(filename_override or input_file_name()),
        }
    )


def add_version_flags(
    df: DataFrame,
    partition_by_col: str,
    meta_ingestion_date_col: str = "_bronze_insert_ts",
) -> DataFrame:
    """Adds 3 boolean columns which can be used in concert to get a current snapshop of a table as well as it's versions over time.
        {
            _initial: True for the first instance of the partitioned column value,
            _latest: True for the most recent instance of the partition column value,
            _update: True for the first instance of row with a given partition column value
                    where it is not flagged as _initial.
        }

    Args:
        df (DataFrame): _description_
        partition_by_col (str): _description_
        meta_ingestion_date_col (str, optional): _description_. Defaults to "_bronze_insert_ts".

    Returns:
        DataFrame: _description_
    """
    hash_partition = Window().partitionBy("_row_hash")
    primary_partition = Window().partitionBy(partition_by_col)

    versioned_df = df.withColumns(
        {
            "_initial": when(
                (
                    df[meta_ingestion_date_col]
                    == min(meta_ingestion_date_col).over(primary_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
            "_latest": when(
                (
                    df[meta_ingestion_date_col]
                    == max(meta_ingestion_date_col).over(primary_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
            "_update": when(
                (
                    df[meta_ingestion_date_col]
                    != min(meta_ingestion_date_col).over(primary_partition)
                )
                & (
                    df[meta_ingestion_date_col]
                    == min(meta_ingestion_date_col).over(hash_partition)
                ),
                True,
            ).otherwise(
                False,
            ),
        }
    )

    return versioned_df


# def add_deleted_transaction_flag(
#     input_df: DataFrame,
#     transaction_date_col: str,
#     meta_ingestion_date_col: str = "_bronze_insert_ts",
# ):
#     """This function assumes an accurate `_latest` flag and that each ingestion of
#     `transaction_date_col` is complete, so if something is included in one ingestion and
#     "missing" from the next it can reasonably be inferred to have been deleted.

#     """
#     date_partition = Window().partitionBy(transaction_date_col)

#     latest_df = (
#         input_df.select(
#             input_df["_row_hash"].alias("latest_hash"),
#             when(
#                 input_df[meta_ingestion_date_col]
#                 == max(meta_ingestion_date_col).over(date_partition),
#                 True,
#             )
#             .otherwise(False)
#             .alias("most_recent"),
#         )
#         .filter("_latest = true")
#         .filter("most_recent = true")
#         .drop("most_recent")
#         .distinct()
#     )

#     join_df = input_df.join(
#         latest_df, on=input_df["_row_hash"] == latest_df["latest_hash"], how="fullouter"
#     )

#     output_df = join_df.withColumn(
#         "_deleted", when((join_df["latest_hash"].isNull()), True).otherwise(False)
#     ).drop("latest_hash")

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
