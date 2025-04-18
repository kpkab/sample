# app/config.py
from pydantic import BaseSettings
import os

class Settings(BaseSettings):
    # Database settings
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "postgresql://iceberg:iceberg@localhost:5432/iceberg")
    
    # Application settings
    API_V1_PREFIX: str = "/v1"
    DEFAULT_WAREHOUSE: str = "s3://300289082521-my-warehouse/dev/"
    
    # JWT token settings (optional, for OAuth)
    SECRET_KEY: str = os.environ.get("SECRET_KEY", "your-secret-key")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    class Config:
        env_file = ".env"

settings = Settings()