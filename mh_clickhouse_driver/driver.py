import logging
import os

import numpy as np
from clickhouse_driver import Client
from pandas import DataFrame


class MHClickhouseDriver():
    def __init__(self):
        self.client = Client(host=os.getenv('CLICKHOUSE_HOST'),
                             port=os.getenv('CLICKHOUSE_PORT'),
                             database=os.getenv('CLICKHOUSE_DATABASE'),
                             user=os.getenv('CLICKHOUSE_USER'),
                             password=os.getenv('CLICKHOUSE_PASSWORD'),
                             secure='True',
                             settings={'use_numpy': True})
        self.logger = logging.getLogger("MHClickhouseDriver")

    FIELD_TYPES = {np.dtype('int64'): 'Int64',
                   np.dtype('<M8[ns]'): 'DateTime',
                   np.dtype('O'): 'String',
                   np.dtype('bool'): 'UInt8',
                   np.dtype('float64'): 'Float'}

    def get_field(self, name, type):
        field_type = self.FIELD_TYPES.get(type, 'String')
        return f"{name} {field_type}"

    def get_table_fields(self, df: DataFrame):
        fields = [self.get_field(name, type)
                  for name, type in df.dtypes.items()]
        return fields

    def create_table_if_not_exists(self, df: DataFrame, table_name: str,
                                   order_by: str, partition_by: str = None):
        nl = ',\n'
        if partition_by:
            order_by = partition_by
            partition_by_sql = f'PARTITION BY {partition_by}'
        else:
            partition_by_sql = ''

        fields = self.get_table_fields(df)

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

    def upload_df(self, df: DataFrame, destination_table: str,
                  order_by: str, partition_by: str = None):

        self.create_table_if_not_exists(
            df, destination_table, order_by, partition_by)

        db = self.client.connection.database
        count = self.client.insert_dataframe(
            f'INSERT INTO {db}.{destination_table} VALUES',
            df)

        self.logger.info("Inserted %s records" % count)
        return count
