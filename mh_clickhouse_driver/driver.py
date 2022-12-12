import logging
import os
import json
import numpy as np
from clickhouse_driver import Client
from pandas import DataFrame
import pandas as pd
from google.cloud import bigquery
from pathlib import Path


class MHClickhouseDriver():
    def __init__(self):
        self.client = Client(host=os.getenv('CLICKHOUSE_HOST'),
                             port=os.getenv('CLICKHOUSE_PORT'),
                             database=os.getenv('CLICKHOUSE_DATABASE'),
                             user=os.getenv('CLICKHOUSE_USER'),
                             password=os.getenv('CLICKHOUSE_PASSWORD'),
                             secure='True')
        self.logger = logging.getLogger("MHClickhouseDriver")

    def __enter__(self):
        return self

    FIELD_TYPES = {np.dtype('int64'): 'Int64',
                   np.dtype('<M8[ns]'): 'DateTime',
                   np.dtype('datetime64[ns]'): 'DateTime',
                   np.dtype('O'): 'String',
                   np.dtype('bool'): 'UInt8',
                   np.dtype('float64'): 'Float'}

    BQ_FIELD_TYPES = {
        'INTEGER': 'Int64',
        'FLOAT': 'FLOAT',
        'BOOLEAN': 'UInt8',
        'STRING': 'String',
        'DATE': 'Date',
        'TIME': 'DateTime',
        'DATETIME': 'DateTime',
        'TIMESTAMP': 'DateTime'
    }

    def map_field(self, f):
        name = f.name

        if f.field_type == 'RECORD':
            datatype = ',\n'.join([self.map_field(r) for r in f.fields])
            datatype = f'Nested({datatype})'
        else:
            datatype = self.BQ_FIELD_TYPES.get(f.field_type, 'UNKNOWN')
        if f.mode == 'REPEATED':
            datatype = f'Array({datatype})'
        return f"{name} {datatype}"

    def get_field(self, name, type):
        field_type = self.FIELD_TYPES.get(type, 'String')
        return f"{name} {field_type}"

    def get_table_fields(self, df: DataFrame):
        fields = [self.get_field(name, type)
                  for name, type in df.dtypes.items()]
        return fields

    def create_table_if_not_exists_df(self,
                                      df: DataFrame,
                                      table_name: str,
                                      order_by: str,
                                      partition_by: str = None):

        fields = self.get_table_fields(df)
        self.create_table_if_not_exists(fields,
                                        table_name,
                                        order_by,
                                        partition_by)

    def create_table_if_not_exists(self,
                                   fields: list[str],
                                   table_name: str,
                                   order_by: str,
                                   partition_by: str = None):
        nl = ',\n'
        if partition_by:
            order_by = partition_by
            partition_by_sql = f'PARTITION BY {partition_by}'
        else:
            partition_by_sql = ''

        query = f'''CREATE TABLE IF NOT EXISTS
        {self.client.connection.database}.{table_name}
        (
        {nl.join(fields)}
        )
        ENGINE = MergeTree()
        {partition_by_sql}
        ORDER BY {order_by};'''
        self.logger.info("Creating table\n%s" % query)
        self.client.execute(query)

    def clean_data(self, table, where_query, db: str = None):
        if not db:
            db = self.client.connection.database
        exists = self.client.execute(f'EXISTS {db}.{table}')

        if exists[0][0] == 1:
            delete_query = f"""ALTER TABLE {db}.{table} delete
                                where {where_query}"""
            self.client.execute(delete_query)

    def fix_object_dtype(self, df: DataFrame):
        for name, type in df.dtypes.items():
            if type == np.dtype('O'):
                df[name] = df[name].astype(str)

    def get_bq_table_fields(self,
                            bq_table: bigquery.Table):
        fields = [self.map_field(f) for f in bq_table.schema]
        return fields

    def create_table_if_not_exists_bq(self,
                                      bq_table: bigquery.Table,
                                      table_name: str,
                                      order_by: str,
                                      partition_by: str = None):

        fields = self.get_bq_table_fields(bq_table)
        self.create_table_if_not_exists(fields,
                                        table_name,
                                        order_by,
                                        partition_by)

    def upload_bq_json(self,
                       destination_table: str,
                       bq_table: bigquery.Table,
                       source_file: str,
                       order_by: str,
                       partition_by: str = None,
                       clean: str = None,
                       db: str = None):
        self.create_table_if_not_exists_bq(bq_table,
                                           destination_table,
                                           order_by,
                                           partition_by)

        if clean:
            self.clean_data(destination_table,  clean, db)

        file = Path(source_file).open('rb')
        js = json.load(file)
        if not db:
            db = self.client.connection.database

        query = f"INSERT INTO {db}.{destination_table}"
        self.client.execute(query, [js])

    def upload_df_only(self, destination_table: str,
                       df: DataFrame, db: str = None):
        if not db:
            db = self.client.connection.database
        count = self.client.insert_dataframe(
            f'INSERT INTO {db}.{destination_table} VALUES',
            df)

        self.logger.info("Inserted %s records" % count)
        return count

    def upload_df(self, df: DataFrame, destination_table: str,
                  order_by: str, partition_by: str = None, clean: str = None):
        self.fix_object_dtype(df)
        self.create_table_if_not_exists_df(
            df, destination_table, order_by, partition_by)

        if clean:
            self.clean_data(destination_table,  clean)

        count = self.upload_df_only(destination_table, df)
        return count

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.disconnect()
