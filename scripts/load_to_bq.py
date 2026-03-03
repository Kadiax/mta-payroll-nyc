import os
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils import config, gcp_clients, gcs_logger

# === Static config ===
SCRIPT_NAME = "load_to_bq"

logger, log_stream = gcs_logger.GCSLogger.setup_logging()

def table_exists(bq_client: bigquery.Client, table_id: str) -> bool:
    """Checks if a BigQuery table exists."""
    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False

def get_already_loaded_files(bq_client: bigquery.Client, table_id: str, location: str) -> set[str]:
    """Retrieves list of files already processed to ensure idempotency."""
    if not table_exists(bq_client, table_id):
        return set()
    
    # We use the 'source_file' column to track ingestion history
    sql = f"SELECT DISTINCT source_file FROM `{table_id}` WHERE source_file IS NOT NULL"
    try:
        rows = bq_client.query(sql, location=location).result()
        return {r.source_file for r in rows}
    except Exception as e:
        logger.warning(f"Could not retrieve loaded files list: {e}")
        return set()

def read_schema(schema_file_name: str) -> list:
    """Reads BigQuery schema from JSON file in the scripts/schemas directory."""
    current_dir = os.path.dirname(__file__)
    schema_path = os.path.join(current_dir, 'schemas', schema_file_name)
    
    with open(schema_path, 'r') as f:
        schema_json = json.load(f)
    return [bigquery.SchemaField.from_api_repr(field) for field in schema_json]

def _load_to_temp(bq_client, uri, temp_id, schema):
    """Loads CSV from GCS to a temporary table."""
    load_cfg = bigquery.LoadJobConfig(
        source_format="CSV", schema=schema,
        write_disposition="WRITE_TRUNCATE", skip_leading_rows=1
    )
    bq_client.load_table_from_uri(uri, temp_id, job_config=load_cfg).result()

def _ensure_raw_table(bq_client, table_id, schema):
    """Creates the final table with audit columns if it doesn't exist."""
    if not table_exists(bq_client, table_id):
        full_schema = schema + [
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("raw_ingested_at", "TIMESTAMP")
        ]
        bq_client.create_table(bigquery.Table(table_id, schema=full_schema))

def _move_to_raw(bq_client, fname, table_id, temp_id, location):
    """Executes the SQL move from temp to raw."""
    sql = f"INSERT INTO `{table_id}` SELECT *, @fname, CURRENT_TIMESTAMP() FROM `{temp_id}`"
    params = [bigquery.ScalarQueryParameter("fname", "STRING", fname)]
    bq_client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=location).result()

def _process_single_file(bq_client, fname, table_id, temp_id, cfg, schema):
    """Orchestrates the loading of a single file."""
    uri = f"gs://{cfg.gcp.bucket_name}/{cfg.source.raw_prefix}{fname}"
    try:
        _load_to_temp(bq_client, uri, temp_id, schema)
        _ensure_raw_table(bq_client, table_id, schema)
        _move_to_raw(bq_client, fname, table_id, temp_id, cfg.gcp.location)
    finally:
        bq_client.delete_table(temp_id, not_found_ok=True)

def reconcile_load_integrity(storage_client, bq_client, cfg, table_id):
    """
    Compares file count in GCS vs source_file count in BigQuery.
    Ensures total integrity of the Bronze layer.
    """
    # 1. Count files in GCS
    bucket = storage_client.bucket(cfg.gcp.bucket_name)
    blobs = bucket.list_blobs(prefix=cfg.source.raw_prefix)
    gcs_count = len([b for b in blobs if b.name.endswith(".csv")])

    # 2. Count distinct source_files in BQ
    sql = f"SELECT COUNT(DISTINCT source_file) as file_count FROM `{table_id}`"
    query_result = bq_client.query(sql, location=cfg.gcp.location).result()
    bq_count = next(query_result).file_count

    # 3. Log results
    raw_table = bq_client.get_table(table_id)
    logger.info(f"Reconciliation: GCS={gcs_count} files | BQ={bq_count} files ({raw_table.num_rows} rows)")

    if gcs_count != bq_count:
        msg = f"Reconciliation failed! GCS has {gcs_count} files but BQ has {bq_count}."
        logger.error(msg)
        raise ValueError(msg)

    logger.info("✅ Load Reconciliation Successful.")
    return raw_table.num_rows

def run_load_pipeline(cfg, bq_client, storage_client):
    """Orchestrates the identification and loading of new GCS files."""
    # 1. Prepare Identifiers
    dataset_id = cfg.gcp.datasets[0] 
    full_table_id = f"{cfg.gcp.project_id}.{dataset_id}.{cfg.source.raw_table_name}"
    temp_table_id = f"{full_table_id}_temp"

    # 2. Identify new files (Idempotency check)
    bucket = storage_client.bucket(cfg.gcp.bucket_name)
    blobs = bucket.list_blobs(prefix=cfg.source.raw_prefix)
    gcs_files = {b.name.split("/")[-1] for b in blobs if b.name.endswith(".csv")}
    
    loaded_files = get_already_loaded_files(bq_client, full_table_id, cfg.gcp.location)
    files_to_process = sorted(list(gcs_files - loaded_files))

    if not files_to_process:
        logger.info("No new files found in GCS. Skipping load.")
        return

    # 3. Load Schema and Process
    schema = read_schema(cfg.source.raw_schema_name)
    for fname in files_to_process:
        _process_single_file(bq_client, fname, full_table_id, temp_table_id, cfg, schema)
        logger.info(f"Successfully loaded {fname} to BigQuery.")

    # 4. Reconcile and Log Final State
    reconcile_load_integrity(storage_client, bq_client, cfg, full_table_id)

def main():
    """Entry point: Setup clients and trigger the pipeline."""
    cfg = config.Config.load_config(logger)
    storage_client = gcp_clients.get_gcs_client(cfg.gcp.project_id)
    gcs_log = gcs_logger.GCSLogger(storage_client, cfg.gcp.bucket_name, cfg.gcp.gcs_log_folder)
    bq_client = bigquery.Client(project=cfg.gcp.project_id)

    try:
        logger.info(f"Starting {SCRIPT_NAME} for project: {cfg.gcp.project_id}")
        run_load_pipeline(cfg, bq_client, storage_client)
    except Exception as e:
        logger.exception(f"Load process failed: {e}")
        raise
    finally:
        gcs_log.upload(log_stream, SCRIPT_NAME)

if __name__ == "__main__":
    main()