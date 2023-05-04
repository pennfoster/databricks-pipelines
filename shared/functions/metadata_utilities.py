import requests

from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


def insert_bronze_metadata_columns(df: DataFrame):
    return df.withColumns(
        {
            "_bronze_insert_ts": current_timestamp(),
            "_raw_file_source": input_file_name(),
            "_code_version": lit(get_current_repo_branch()),
        }
    )


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
