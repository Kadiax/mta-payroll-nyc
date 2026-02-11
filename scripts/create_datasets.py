# create_dataset.py
import os
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from utils import config, gcp_clients, gcs_logger

# === Static config ===
SCRIPT_NAME = "create_datasets"

# === Logging setup ===
logger, log_stream = gcs_logger.GCSLogger.setup_logging()


def create_bigquery_dataset(bq_client: bigquery.Client, project_id: str, location: str, dataset_name: str):
    """Create a BigQuery dataset if it does not exist."""
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        bq_client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = location
        bq_client.create_dataset(ds, timeout=30)
        logger.info(f"Created dataset {dataset_id} in location {location}.")

def main():
    cfg = config.Config.load_config(logger)
    bq_client = gcp_clients.get_bq_client(cfg.project_id)
    storage_client = gcp_clients.get_gcs_client(cfg.project_id)
    gcs_logger_instance = gcs_logger.GCSLogger(storage_client, cfg.bucket_name, cfg.gcs_log_folder)

    logger.info("Starting dataset creation process...")
    for name in cfg.datasets:
        create_bigquery_dataset(bq_client, cfg.project_id, cfg.location, name)
    logger.info("All dataset checks/creations completed successfully.")

    gcs_logger_instance.upload(log_stream, SCRIPT_NAME)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Unexpected error during dataset creation: {e}")
        print(f"[warn] could not upload logs to GCS due to early failure {e}")
