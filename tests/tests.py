import glob
import os
import unittest

import dotenv
from clickhouse_driver import Client
from generate_fake_df import generate_fake_dataframe
from pandas import read_csv

from mh_clickhouse_driver.driver import MHClickhouseDriver

dotenv.load_dotenv()


class TestMHClickhouseDriver(unittest.TestCase):
    def setUp(self) -> None:
        self.client = Client(host=os.getenv('CLICKHOUSE_HOST'),
                             port=os.getenv('CLICKHOUSE_PORT'),
                             database=os.getenv('CLICKHOUSE_DATABASE'),
                             user=os.getenv('CLICKHOUSE_USER'),
                             password=os.getenv('CLICKHOUSE_PASSWORD'),
                             secure='True',
                             settings={'use_numpy': True})
        db = self.client.connection.database
        tables = self.client.execute(f'SHOW TABLES IN {db};')
        for table in tables[0]:
            query = (f'DROP TABLE IF EXISTS {db}.{table};')
            self.client.execute(query)
        return super().setUp()

    def tearDown(self) -> None:

        self.client.disconnect()
        return super().tearDown()

    def test_dataframe_upload(self):
        df = generate_fake_dataframe(size=100, cols="cifcdtb")
        table_name = 'test_dataframe_upload'

        driver = MHClickhouseDriver()
        count = driver.upload_df(df, table_name, 'column_5_datetime')

        self.assertEqual(count, 100)

        db = self.client.connection.database
        query = f"SELECT COUNT(*) FROM {db}.{table_name};"
        result = self.client.execute(query)
        self.assertEqual(result[0][0], 100)

    def test_testdata_uploads(self):
        driver = MHClickhouseDriver()
        for filepath in glob.glob(os.path.join('tests/test_data', '*.csv')):
            df = read_csv(filepath)
            print(df.head())
            names = os.path.basename(filepath).split('.')[0].split('_')

            count = driver.upload_df(df, names[0], names[1])

            self.assertEqual(len(df), count)


if __name__ == "__main__":
    unittest.main()
