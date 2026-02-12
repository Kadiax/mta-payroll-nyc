import yaml
import logging
from pathlib import Path
from typing import List
from pydantic import BaseModel, field_validator

class BaseConfig(BaseModel, frozen=True):
    """Base class to avoid code repetition."""
    @field_validator("*", mode="before")
    @classmethod
    def check_not_empty(cls, v):
        if isinstance(v, str) and not v.strip():
            raise ValueError("String fields cannot be empty")
        return v

class GCPConfig(BaseConfig):
    project_id: str
    location: str
    bucket_name: str
    gcs_log_folder: str

class SourceConfig(BaseConfig):
    url: str
    raw_prefix: str
    salt: str
    raw_table_name: str  
    raw_schema_name: str

    @field_validator("url")
    @classmethod
    def validate_url_format(cls, v: str) -> str:
        """Specific check to ensure the URL is secure and valid."""
        if not v.startswith("https://"):
            raise ValueError("The MTA URL must start with 'https://' for security")
        return v

class Config(BaseConfig):
    gcp: GCPConfig
    source: SourceConfig
    datasets: List[str]

    @field_validator("datasets")
    @classmethod
    def check_datasets_not_empty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("The 'datasets' list cannot be empty")
        return v

    @classmethod
    def load_config(cls, logger: logging.Logger = None) -> "Config":
        """Loads, validates, and returns the configuration from the YAML file."""
        config_path = Path("./config.yaml")
        
        if logger:
            logger.info(f"Loading configuration from: {config_path}")
        
        if not config_path.exists():
            error_msg = f"Config file not found: {config_path}"
            if logger:
                logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
        
        # Pydantic valide tout ici : les types, le contenu et l'immuabilité (frozen)
        config = cls(**config_dict)
        
        if logger:
            logger.info("Configuration loaded and validated successfully with Pydantic")
        
        return config