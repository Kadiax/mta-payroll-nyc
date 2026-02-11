import io
import hashlib
import requests
import pandas as pd
from datetime import datetime, UTC
from utils import config, gcp_clients, gcs_logger

# === Static config ===
SCRIPT_NAME = "extract_and_anonymize"

logger, log_stream = gcs_logger.GCSLogger.setup_logging()

def handle_missing_names(df: pd.DataFrame) -> pd.DataFrame:
    """Handles null values and logs quality issues."""
    null_count = df['Name'].isnull().sum()
    if null_count > 0:
        logger.warning(f"Found {null_count} rows with missing names.")
    
    df['Name'] = df['Name'].fillna("UNKNOWN_EMPLOYEE").str.strip()
    return df

def hash_string(value: str, salt: str) -> str:
    """Atomic function to hash a single string with a salt."""
    return hashlib.sha256(f"{value}{salt}".encode()).hexdigest()[:16]

def anonymize_data(df: pd.DataFrame, salt: str) -> pd.DataFrame:
    """
    Orchestrates the anonymization process
    """
    if 'Name' not in df.columns:
        return df

    # 1. Clean
    df = handle_missing_names(df)

    # 2. Transform
    logger.info("Applying salted hashing to employee names...")
    df['employee_id'] = df['Name'].apply(lambda x: hash_string(x, salt))

    # 3. Finalize
    return df.drop(columns=['Name'])

def download_mta_data(url: str) -> pd.DataFrame:
    """Downloads the raw CSV from MTA Open Data Portal."""
    logger.info(f"Downloading data from: {url}")
    response = requests.get(url, timeout=600)
    response.raise_for_status()
    return pd.read_csv(io.BytesIO(response.content))

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, prefix: str, storage_client):
    """Converts DataFrame to CSV and uploads to GCS."""
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    target_path = f"{prefix}payroll_{timestamp}.csv"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(target_path)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    logger.info(f"Successfully uploaded to gs://{bucket_name}/{target_path}")

def main():
    # 1. Initialization
    cfg = config.Config.load_config(logger)
    storage_client = gcp_clients.get_gcs_client(cfg.gcp.project_id)
    gcs_log = gcs_logger.GCSLogger(storage_client, cfg.gcp.bucket_name, cfg.gcp.gcs_log_folder)

    try:
        # 2. Extract
        raw_df = download_mta_data(cfg.mta.url)
        
        # 3. Transform (Anonymize)
        clean_df = anonymize_data(raw_df, cfg.mta.salt) 
        
        # 4. Load
        upload_to_gcs(clean_df, cfg.gcp.bucket_name, cfg.mta.raw_prefix, storage_client)
        
        logger.info("ETL Process: Extraction and Anonymization completed successfully.")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise
    finally:
        gcs_log.upload(log_stream, SCRIPT_NAME)

if __name__ == "__main__":
    main()