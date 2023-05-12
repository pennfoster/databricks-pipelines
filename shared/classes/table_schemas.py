from pyspark import pandas as ps
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from shared.classes import BaseClass
from shared.functions.azure_utilities import get_mount_paths

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class TableSchemas(BaseClass):
    def __init__(self, data_source, table_name):
        self.data_source = data_source.lower()
        self.table_name = table_name.lower()
        self.paths = get_mount_paths(data_source)
        self.hive_tableschemas_path = f"bronze_{self.data_source}.tableschemas"
        #                 id bigint GENERATED ALWAYS AS IDENTITY
        self.table_ddl = f"""
            {self.hive_tableschemas_path} (
                data_source string
                , table_name string
                , select_clause string
                , dest_column_name string
                , data_type string
                , column_index int
                , is_active boolean
                , requires_rebuild boolean
                , is_custom boolean
                , record_insert_date timestamp
                , record_update_date timestamp
            )
            using delta
            location '{self.paths.bronze}/tableschemas'
        """

    def create_table(self, recreate=False):
        if recreate:
            dbutils.fs.rm(f"{self.paths.bronze}/tableschemas", recurse=True)

        spark.sql(f"create table if not exists {self.table_ddl}")
        return

    def new_column_inserts(self, columns, default_dtype="string"):
        self.create_table()

        columns = [i.lower() for i in columns]
        query = spark.sql(
            f"select distinct select_clause from bronze_{self.data_source}.tableschemas where lower(table_name) = '{self.table_name}'"
        ).toPandas()
        existing_cols = query.iloc[:, 0].to_list()
        new_columns = list(set(columns) ^ set(existing_cols))

        if new_columns:
            col_inserts = ", ".join(
                [
                    f"('{self.data_source}', '{self.table_name}', '{i}', '{i}', '{default_dtype}', null, true, true, false, current_timestamp(), current_timestamp())"
                    for i in new_columns
                ]
            )
            insert_query = f"""
                insert into bronze_{self.data_source}.tableschemas
                (data_source, table_name, select_clause, dest_column_name, data_type, column_index, is_active, requires_rebuild, is_custom, record_insert_date, record_update_date)
                values
                {col_inserts}
            """
            print(insert_query)
            spark.sql(insert_query)

            return new_columns
        return None

    def get_select_clause(self, nullif=None):
        df = ps.read_delta(f"{self.paths.bronze}/tableschemas")
        df = df[(df["is_active"] == True) & (df["table_name"] == self.table_name)]
        df.sort_values(by=["dest_column_name"], inplace=True)

        query_parts = []
        for i in df.index.to_numpy():
            row = df.loc[int(i), :]
            if nullif is not None:
                part = f"nullif(cast({row['select_clause']} as {row['data_type']}), '{nullif}')"
            else:
                part = f"cast({row['select_clause']} as {row['data_type']})"
            part += f" as {row['dest_column_name']}"
            query_parts.append(part)

        return ", ".join(query_parts)

    def get_create_clause(self):
        df = ps.read_delta(f"{self.paths.bronze}/tableschemas")
        df = df[(df["is_active"] == True) & (df["table_name"] == self.table_name)]
        df.sort_values(by=[f"dest_column_name"], inplace=True)

        query_parts = []
        for i in df.index.to_numpy():
            row = df.loc[int(i), :]
            part = f"{row['dest_column_name']} {row['data_type']}"
            query_parts.append(part)

        return ", ".join(query_parts)
