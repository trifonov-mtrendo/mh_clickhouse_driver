import glob
import os
import unittest

import dotenv
from clickhouse_driver import Client
from generate_fake_df import generate_fake_dataframe
from pandas import read_csv
from google.cloud import bigquery
from mh_clickhouse_driver.driver import MHClickhouseDriver
from pathlib import Path

from bq_utils import create_dataset, get_client

dotenv.load_dotenv()

AUTH = '{"token": "ya29.A0AVA9y1sUjuzdVJuwzR7eicABVD_DnJbHgL-8RXVmPVUBE_O6rKXJPPyFpu8EjrOcAwTew_0TdUtxFfdJp9UmC5zcNiLPvEpQnhEO-Vw1PmrdylzuRYLwVE6T7IEeJleeLPj8FWt8uz959yWqOou162Y6B_pTaCgYKATASATASFQE65dr8aLVxQw3NXjleHdlSvXwOcg0163", "refresh_token": "1//0c3m8swKaOeO_CgYIARAAGAwSNwF-L9IrBmUwU7KkB4NrU65I3wRINJC9ZVveBO1GhAFHUCQQdJh8u6lTSaW4eBOus6d8D7kb1Vk", "id_token": null, "token_uri": "https://oauth2.googleapis.com/token", "client_id": "470299993400-8kv9f21ofoagjfm7ke7u008fp8n9d557.apps.googleusercontent.com", "client_secret": "GD8TNO00_rmXQL3iElSev4yr", "scopes": ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/devstorage.read_write"], "expiry": "2022-08-18 11:18:47"}'
BASE_DIR = Path(__file__).resolve().parent


class TestMHClickhouseDriver(unittest.TestCase):
    project_id = "aerobic-form-344316"
    dataset_id = f"{project_id}.test_dataset"
    tables = []

    def setUp(self) -> None:

        self.bq_client = get_client(self.project_id, AUTH)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )
        create_dataset(self.bq_client, self.dataset_id)
        for filepath in glob.glob(
                os.path.join('tests/bq_test_data',
                             '*.json')):

            source_file = Path(filepath).open('rb')
            table_name = os.path.basename(filepath).split('.')[0]
            table_id = f'{self.dataset_id}.bq2ch_{table_name}'
            self.tables.append((table_id, filepath))
            job = self.bq_client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

            job.result()  # Waits for the job to complete.

        self.client = Client(host=os.getenv('CLICKHOUSE_HOST'),
                             port=os.getenv('CLICKHOUSE_PORT'),
                             database=os.getenv('CLICKHOUSE_DATABASE'),
                             user=os.getenv('CLICKHOUSE_USER'),
                             password=os.getenv('CLICKHOUSE_PASSWORD'),
                             secure='True',
                             settings={'use_numpy': True})
        db = self.client.connection.database
        tables = self.client.execute(f'SHOW TABLES IN {db};')
        for table in tables:
            query = (f'DROP TABLE IF EXISTS {db}.{table[0]};')
            self.client.execute(query)

        self.driver = MHClickhouseDriver()
        return super().setUp()

    def tearDown(self) -> None:
        # self.bq_client.delete_dataset(
        #     self.dataset_id, delete_contents=True, not_found_ok=True
        # )
        # self.client.disconnect()
        self.driver.client.disconnect()
        return super().tearDown()

    def test_plain_table(self):
        table_id = f"{self.dataset_id}.bq2ch_1_plain_table"
        table_name = table_id.split('.')[2]
        table = self.bq_client.get_table(table_id)
        filename = "tests/bq_test_data/1_plain_table.json"
        row_count = self.driver.upload_bq_json(
            table_name,
            table,
            filename,
            order_by='date'
        )

        db = os.getenv('CLICKHOUSE_DATABASE')
        ch_table_id = f"{db}.{table_name}"
        query = f"SELECT COUNT(*) FROM {ch_table_id};"
        result = self.client.execute(query)

        self.assertEqual(result[0][0], row_count)

    def test_simple_nesting(self):
        table_id = f"{self.dataset_id}.bq2ch_2_simple_nesting"
        table_name = table_id.split('.')[2]
        table = self.bq_client.get_table(table_id)
        filename = "tests/bq_test_data/2_simple_nesting.json"
        row_count = self.driver.upload_bq_json(
            table_name,
            table,
            filename,
            order_by='date'
        )

        db = os.getenv('CLICKHOUSE_DATABASE')
        ch_table_id = f"{db}.{table_name}"
        query = f"SELECT COUNT(*) FROM {ch_table_id};"
        result = self.client.execute(query)

        self.assertEqual(result[0][0], row_count)

    def test_complex_nesting(self):
        table_id = f"{self.dataset_id}.bq2ch_3_complex_nesting"
        table_name = table_id.split('.')[2]
        table = self.bq_client.get_table(table_id)
        filename = "tests/bq_test_data/3_complex_nesting.json"
        row_count = self.driver.upload_bq_json(
            table_name,
            table,
            filename,
            order_by='date'
        )

        db = os.getenv('CLICKHOUSE_DATABASE')
        ch_table_id = f"{db}.{table_name}"
        query = f"SELECT COUNT(*) FROM {ch_table_id};"
        result = self.client.execute(query)

        self.assertEqual(result[0][0], row_count)


if __name__ == "__main__":
    unittest.main()
