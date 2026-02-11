import yaml
import logging
from pathlib import Path
from typing import List
from pydantic import BaseModel, field_validator

class GCPConfig(BaseModel, frozen=True):
    """GCP-specific configuration (Immutable)."""
    project_id: str
    location: str
    bucket_name: str
    gcs_log_folder: str

    @field_validator("*")
    @classmethod
    def check_not_empty(cls, v: str) -> str:
        if isinstance(v, str) and not v.strip():
            raise ValueError("String fields cannot be empty")
        return v

class Config(BaseModel, frozen=True):
    """Global configuration class (Immutable)."""
    gcp: GCPConfig
    datasets: List[str]

    @field_validator("datasets")
    @classmethod
    def check_datasets(cls, v: List[str]) -> List[str]:
        # Vérifie que la liste n'est pas vide
        if not v:
            raise ValueError("The 'datasets' list cannot be empty")
        # Vérifie que chaque nom de dataset est valide
        for name in v:
            if not name.strip():
                raise ValueError("Dataset names in the list cannot be empty")
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