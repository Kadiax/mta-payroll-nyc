import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils import config, gcp_clients, gcs_logger

# === Static config ===
SCRIPT_NAME = "create_datasets"

# === Logging setup ===
# We initialize the base logger. GCS upload happens at the end.
logger, log_stream = gcs_logger.GCSLogger.setup_logging()

def create_bigquery_dataset(bq_client: bigquery.Client, project_id: str, location: str, dataset_name: str):
    """Create a BigQuery dataset if it does not exist."""
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        bq_client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        logger.info(f"Dataset {dataset_id} not found. Creating it...")
        ds = bigquery.Dataset(dataset_id)
        ds.location = location
        bq_client.create_dataset(ds, timeout=30)
        logger.info(f"Successfully created dataset {dataset_id} in {location}.")

def main():
    # 1. Load validated config via Pydantic
    cfg = config.Config.load_config(logger)
    
    # 2. Initialize GCP Clients
    # Note: These use cfg.gcp sub-object from your new Pydantic model
    bq_client = gcp_clients.get_bq_client(cfg.gcp.project_id)
    storage_client = gcp_clients.get_gcs_client(cfg.gcp.project_id)
    
    # 3. Setup GCS Logging instance
    gcs_logger_instance = gcs_logger.GCSLogger(
        storage_client, 
        cfg.gcp.bucket_name, 
        cfg.gcp.gcs_log_folder
    )

    logger.info("Starting dataset creation process...")
    
    # 4. Iterate over dataset list from config
    for name in cfg.datasets:
        create_bigquery_dataset(
            bq_client, 
            cfg.gcp.project_id, 
            cfg.gcp.location, 
            name
        )
        
    logger.info("All dataset checks/creations completed successfully.")

    # 5. Final upload of logs to GCS
    gcs_logger_instance.upload(log_stream, SCRIPT_NAME)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"Unexpected error during execution: {e}")
        # Final safety print if GCS upload fails or doesn't trigger
        print(f"[FATAL] Script failed. Check local logs if GCS upload was not possible. Error: {e}")