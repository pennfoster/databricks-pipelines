import requests

from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame, Window, Column
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    when,
    min,
    max,
)

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


def add_insert_data(df: DataFrame):
    return df.withColumns(
        {
            "_bronze_insert_ts": current_timestamp(),
            "_raw_file_source": input_file_name(),
            "_code_version": lit(get_current_repo_branch()),
        }
    )


from pyspark.sql import DataFrame, Column
from typing import List, Union


def add_data_version_flags(
    df: DataFrame,
    internal_date_col: Union[Column, str],
    metadata_date_col: Union[Column, str],
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
                (metadata_date_col == min(metadata_date_col).over(current_window)),
                True,
            ).otherwise(
                False,
            ),
            "_most_recent_data_for_date": when(
                (metadata_date_col == max(metadata_date_col).over(current_window)),
                True,
            ).otherwise(
                False,
            ),
            "_deleted_at_source": when(
                (metadata_date_col != max(metadata_date_col).over(current_window))
                & (metadata_date_col == max(metadata_date_col).over(outdated_window)),
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
