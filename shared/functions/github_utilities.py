import requests
from typing import List, Tuple, Literal

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
dbutils = DBUtils(spark)

from shared.functions.azure_utilities import get_key_vault_scope


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


# ! This code is no longer inuse and can be deprecated. Left here in case for now. (2023-04-24 Noam Blanks)
def get_pr_files(
    repo: str, pr_number: int, directory_path: str = None
) -> List[
    Tuple[
        str,
        Literal[
            "added",
            "removed",
            "modified",
            "renamed",
            "copied",
            "changed",
            "unchanged",
        ],
    ]
]:
    changed_files = []
    url = f"https://api.github.com/repos/pennfoster/{repo}/pulls/{pr_number}/files"
    with requests.Session() as s:
        s.headers = {
            **s.headers,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        params = {"per_page": 100, "page": 1}
        while True:
            response = s.get(url, params=params)
            print(response.request.url)
            print(response.request.body)
            response.raise_for_status()
            if response.json() == []:
                break
            changed_files.extend(
                [(file["filename"], file["status"]) for file in response.json()]
            )
            params["page"] += 1

    if directory_path:
        changed_files = [t for t in changed_files if t[0].startswith(directory_path)]
    return changed_files
