# config/settings.py
"""
Configuration management for the GA4 pipeline project.
This is project-specific configuration, not part of the installable package.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Project settings using environment variables"""
    
    # Google Cloud Platform
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    GA4_DATASET_ID = os.getenv("GA4_DATASET_ID")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    # AWS
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_PREFIX = os.getenv("S3_PREFIX", "bronze/ga4")
    
    # Pipeline
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
    
    # Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    QUERIES_DIR = PROJECT_ROOT / "config" / "queries"
    
    @classmethod
    def validate(cls):
        """Validate required settings"""
        required = [
            "GCP_PROJECT_ID",
            "GA4_DATASET_ID", 
            "S3_BUCKET"
        ]
        
        missing = [key for key in required if getattr(cls, key) is None]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")
        
        return True


# Create global settings instance
settings = Settings()


# config/__init__.py  
"""Configuration module for the GA4 pipeline project"""
from .settings import settings

__all__ = ["settings"]