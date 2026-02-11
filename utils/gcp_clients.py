from google.cloud import bigquery, storage


def get_bq_client(project_id: str):
    return bigquery.Client(project=project_id)

def get_gcs_client(project_id: str):
    return storage.Client(project=project_id)
