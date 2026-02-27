import os
import subprocess
from utils import config, gcp_clients, gcs_logger

# === Static config ===
SCRIPT_NAME = "transform"

# Setup logging initial (console stream)
logger, log_stream = gcs_logger.GCSLogger.setup_logging()

def execute_dbt_build(working_dir: str) -> bool:
    """Executes the dbt build command in the specified working directory 
    and captures logs in real-time."""
    logger.info("Starting dbt build...")
    try:
        process = subprocess.Popen(
            ["dbt", "build", "--full-refresh"],
            cwd=working_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        for line in process.stdout:
            clean_line = line.strip()
            logger.info(clean_line)
            print(clean_line)

        process.wait()
        return process.returncode == 0
    except Exception as e:
        logger.error(f"Failed to execute dbt build: {e}")
        return False

def attach_internal_dbt_logs(working_dir: str):
    """After dbt execution, attempt to read the internal dbt.log 
    file and attach its contents to our main log stream for comprehensive auditing."""
    log_path = os.path.join(working_dir, "logs", "dbt.log")
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            logger.info("--- ATTACHING INTERNAL DBT.LOG DETAILS ---")
            logger.info(f.read())
    else:
        logger.warning("Internal dbt.log file not found.")

def main():
    # 1. Initialize config and GCS logger
    cfg = config.Config.load_config(logger)
    storage_client = gcp_clients.get_gcs_client(cfg.gcp.project_id)
    
    gcs_logger_instance = gcs_logger.GCSLogger(
        storage_client, 
        cfg.gcp.bucket_name, 
        cfg.gcp.gcs_log_folder
    )
    
    dbt_dir = cfg.gcp.dbt_project_dir

    # 2. Execute dbt build and capture logs in real-time
    success = execute_dbt_build(dbt_dir)
    
    if not success:
        logger.error("dbt build process finished with errors.")

    # 3. Post-processing (Audit)
    attach_internal_dbt_logs(dbt_dir)

    # 4. Final upload of logs to GCS
    gcs_logger_instance.upload(log_stream, SCRIPT_NAME)

    if not success:
        exit(1)

if __name__ == "__main__":
    main()