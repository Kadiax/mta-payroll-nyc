# utils/config.py
import yaml
import logging
from pathlib import Path

class Config:
    """Simple configuration class with type validation."""
    
    def __init__(self, config_dict: dict):
        # Validate and set gcp config
        gcp = config_dict.get("gcp", {})
        self.project_id = self._validate_str(gcp.get("project_id"), "gcp.project_id")
        self.location = self._validate_str(gcp.get("location"), "gcp.location")
        self.bucket_name = self._validate_str(gcp.get("bucket_name"), "gcp.bucket_name")
        self.gcs_log_folder = self._validate_str(gcp.get("gcs_log_folder"), "gcp.gcs_log_folder")
        
        # Validate and set datasets
        self.datasets = self._validate_list(config_dict.get("datasets", []), "datasets")
    
    @staticmethod
    def _validate_str(value, key: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{key} must be str, got {type(value).__name__}")
        if not value:
            raise ValueError(f"{key} cannot be empty")
        return value
    
    @staticmethod
    def _validate_list(value, key: str) -> list:
        if not isinstance(value, list):
            raise TypeError(f"{key} must be list, got {type(value).__name__}")
        if not all(isinstance(item, str) for item in value):
            raise TypeError(f"All items in {key} must be strings")
        return value

    @staticmethod
    def load_config(logger: logging.Logger = None) -> "Config":
        """Loads and validates the pipeline configuration from YAML file."""
        config_path = "./config.yaml"
        
        if logger:
            logger.info(f"Loading configuration from: {config_path}")
        
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
        
        config = Config(config_dict)
        
        if logger:
            logger.info("Configuration loaded and validated successfully")
        
        return config