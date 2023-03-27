from collections import namedtuple

from shared.functions.databricks_utilities import pass_databricks_env


@pass_databricks_env
def get_key_vault_scope(**kwargs):
    env = kwargs["env"]
    key_vault_scopes = {"dev": "kv-datateam-dev001"}

    return key_vault_scopes[env]


@pass_databricks_env
def get_mount_paths(dest_directory: str, **kwargs):
    env = kwargs["env"]

    Paths = namedtuple("Paths", ["landing", "control", "bronze", "silver", "gold"])
    return Paths(
        landing=f"/mnt/sadataraw{env}001_landing/{dest_directory}",
        control=f"/mnt/sadataraw{env}001_control/{dest_directory}",
        bronze=f"/mnt/sadatalakehouse{env}001_bronze/{dest_directory}",
        silver=f"/mnt/sadatalakehouse{env}001_silver/{dest_directory}",
        gold=f"/mnt/sadatalakehouse{env}001_gold/{dest_directory}",
    )
