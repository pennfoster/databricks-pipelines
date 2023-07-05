import logging

from shared.functions.azure_utilities import get_mount_paths

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class BaseClass:
    def __init__(self, **kwargs):
        self.data_source = kwargs.get("data_source")
        if self.data_source:
            self._set_file_paths()

    def _set_file_paths(self):
        if "/" in self.data_source:
            raise ValueError(
                'Data source contains a forward slash ("/") '
                "which may result in unintended subdirectories"
            )
        self.storage_paths = get_mount_paths(self.data_source)

        dbutils.fs.mkdirs(self.storage_paths.landing)
        dbutils.fs.mkdirs(self.storage_paths.control)
        dbutils.fs.mkdirs(self.storage_paths.bronze)
        dbutils.fs.mkdirs(self.storage_paths.silver)
        dbutils.fs.mkdirs(self.storage_paths.gold)
