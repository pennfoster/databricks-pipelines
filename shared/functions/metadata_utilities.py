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


def add_data_version_flags_2(
    df: DataFrame,
    partition_by_col: str,
    internal_date_col: str = None,
    # internal_deleted_col: str = None,
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

    v_df = hash_df.withColumns(
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

    if internal_date_col:
        latest = [
            h[0]
            for h in v_df.select("_row_hash")
            .where("_latest = TRUE")
            .distinct()
            .collect()
        ]
        bc_latest = sc.broadcast(latest)
        internal_date_partition = Window().partitionBy(internal_date_col)
        vd_df = v_df.withColumn(
            "_deleted",
            when(
                (~v_df["_row_hash"].isin(bc_latest.value))
                | (
                    (v_df["_row_hash"].isin(bc_latest.value))
                    & (
                        v_df[meta_ingestion_date_col]
                        != max(meta_ingestion_date_col).over(internal_date_partition)
                    )
                ),
                True,
            ).otherwise(
                False,
            ),
        )
        return vd_df

    return v_df


def add_data_version_flags(
    df: DataFrame,
    internal_date_col: str,
    metadata_date_col: str,
) -> DataFrame:
    """Adds 3 metadata columns to spark DataFrame:
        (
            "_initial_data_for_date",
            "_most_recent_data_for_date",
            "_deleted_at_source"
        )
    This function *requires* that the DataFrame's metadata column names start with an underscore ("_")

    Args:
        df (DataFrame): _description_
        internal_date_col (Union[Column, str]): _description_
        metadata_date_col (Union[Column, str]): _description_

    Returns:
        _type_: _description_
    """
    original_columns = [c for c in df.columns if not c.startswith("_")]

    current_window = Window().partitionBy(internal_date_col)
    outdated_window = Window().partitionBy(original_columns)

    output_df = df.withColumns(
        {
            "_initial_data_for_date": when(
                (df[metadata_date_col] == min(metadata_date_col).over(current_window)),
                True,
            ).otherwise(
                False,
            ),
            "_most_recent_data_for_date": when(
                (df[metadata_date_col] == max(metadata_date_col).over(current_window)),
                True,
            ).otherwise(
                False,
            ),
            "_deleted_at_source": when(
                (df[metadata_date_col] != max(metadata_date_col).over(current_window))
                & (
                    df[metadata_date_col]
                    == max(metadata_date_col).over(outdated_window)
                ),
                True,
            ).otherwise(
                False,
            ),
        }
    )

    return output_df


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
