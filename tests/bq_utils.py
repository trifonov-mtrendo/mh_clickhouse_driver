from google.oauth2.credentials import Credentials
import json
from google.cloud import bigquery


def get_google_credentials(stringified_token: str) -> Credentials:
    token = json.loads(stringified_token)
    creds = Credentials(
        token["token"],
        refresh_token=token["refresh_token"],
        token_uri=token["token_uri"],
        client_id=token["client_id"],
        client_secret=token["client_secret"],
    )

    return creds


def get_client(project_id: str, stringified_token: str) -> bigquery.Client:
    return bigquery.Client(project_id,
                           get_google_credentials(stringified_token))


def create_dataset(client: bigquery.Client,
                   dataset_id: str) -> bigquery.Dataset:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"
    dataset = client.create_dataset(dataset, exists_ok=True)
    return dataset
