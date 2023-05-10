from pyspark import pandas as ps

from shared.classes import BaseClass


class TableSchemas(BaseClass):
    def __init__(self, data_source, table_name):
        self.data_source = data_source.lower()
        self.table_name = table_name.lower()
        self.paths = self.get_mount_paths(data_source)
        self.table_dll = f'''
            bronze_{self.data_source}.tableschemas (
                id long GENERATED ALWAYS AS IDENTITY
                , data_source varchar(100)
                , table_name varchar(100)
                , select_clause varchar(100)
                , dest_column_name varchar(100)
                , data_type varchar(100)
                , column_index int
                , is_active boolean
                , requires_rebuild boolean
                , is_custom boolean
                , record_insert_date timestamp
                , record_update_date timestamp
            )
            using delta
            location 'f"{self.paths.bronze}/tableschemas"'
        '''

    def create_table(self):
        spark.sql(f'create table if not exists {self.table_ddl}')
    
    def recreate_table(self):
        spark.sql(f'create or replace table {self.table_ddl}')

    def new_column_inserts(self, columns, default_dtype='varchar(250)'):
        self.create_table()

        columns = [i.lower() for i in columns]
        query = spark.sql(
            f"select distinct src_column_name from bronze_{self.data_source}.tableschemas where lower(table_name) = '{self.table_name}'"
        ).toPandas()
        existing_cols = query.iloc[:, 0].to_list()
        new_columns = list(set(columns) ^ set(existing_cols))

        if new_columns:
            col_inserts = ', '.join([f"('{self.data_source}', '{self.table_name}', '{i}', '{i}', {default_dtype}, null, true, true, false, current_timestamp(), current_timestamp())" for i in new_columns])
            insert_query = f'''
                insert into bronze_{self.data_source}.tableschemas
                (data_source, table_name, src_column_name, dest_column_name, data_type, column_index, is_active, requires_rebuild, is_custom, record_insert_date, record_update_date)
                values
                {col_inserts}
            '''
            spark.sql(insert_query)

            return new_columns
        return None
    
    def build_select_query(self):
        df = ps.read_delta(f'{self.paths.bronze}/tableschemas')
        df = df[df['is_active']==True]
        df.sort_values(by=['column_index', 'id'])
        query_parts = []
        for i in df.index.to_numpy():
            row = df.iloc[int(i), :]
            part = f"cast({row['src_column_name']} as {row['datatype']}) as {row['dest_column_name']}"
            query_parts.append(part)