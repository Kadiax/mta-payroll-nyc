import io
import logging
from datetime import datetime, UTC
from google.cloud import storage

class GCSLogger:
    """Handles uploading logs to Google Cloud Storage."""
    
    def __init__(self, storage_client: storage.Client, bucket_name: str, gcs_log_folder: str):
        """
        Initialize GCS logger.
        
        Args:
            storage_client: GCS storage client
            bucket_name: GCS bucket name
            gcs_log_folder: Folder path in GCS for logs
        """
        self.storage_client = storage_client
        self.bucket_name = bucket_name
        self.gcs_log_folder = gcs_log_folder
    
    @staticmethod
    def setup_logging():
        """
        Configure logging with a StringIO buffer for cloud storage.
        
        Returns:
            tuple: (logger, log_stream) where logger is the configured logger
                   and log_stream is the StringIO buffer containing the logs
        """
        log_stream = io.StringIO()
        logging.basicConfig(
            stream=log_stream,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        logger = logging.getLogger(__name__)
        return logger, log_stream
    
    def upload(self, log_stream: io.StringIO, script_name: str):
        """Upload the log content to GCS with a timestamped filename."""
        try:
            log_filename = f"{self.gcs_log_folder}{script_name}_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
            bucket = self.storage_client.bucket(self.bucket_name)
            bucket.blob(log_filename).upload_from_string(log_stream.getvalue())
            print(f"Log file uploaded to gs://{self.bucket_name}/{log_filename}")
        except Exception as e:
            # best-effort: do not fail the run because of log upload
            print(f"[warn] failed to upload log to GCS: {e}")
