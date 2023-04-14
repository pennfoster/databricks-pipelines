import logging
from typing import Callable, TypeVar
from typing_extensions import ParamSpec

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

_P = ParamSpec("_P")
_T = TypeVar("_T")


# Decorator to pass current db instance into function as kwarg
def pass_databricks_env(func: Callable[_P, _T]) -> Callable[_P, _T]:
    def _func(*args, **kwargs):
        sc = SparkContext.getOrCreate()

        environment_dict = {
            "6104815453986823": "dev",  # dbw-datateam-dev001
            "7121333149039885": "ADBSmlDev01",  # ADBSmlDev01
            "8282478637069706": "prd",  # dbw-datateam-prd001
            # "2211778133336071": "prd",  # pfcarrusdata
        }
        workspace_id = sc.getConf().get(
            "spark.databricks.clusterUsageTags.clusterOwnerOrgId"
        )

        kwargs["env"] = environment_dict[workspace_id]

        if kwargs["env"] not in ["dev", "prd"]:
            logging.error("Env detection not yet tested for this environment")
            raise ValueError("Code used in environment other than intended")

        return func(*args, **kwargs)

    _func.__wrapped__ = func

    return _func


def flatten_json_string_col_to_struct(input_df: DataFrame, column_name: str):
    nested_schema = spark.read.json(
        input_df.rdd.map(lambda row: row[column_name])
    ).schema
    output_df = input_df.withColumn(
        column_name,
        from_json(col(column_name), nested_schema),
    )

    return output_df
