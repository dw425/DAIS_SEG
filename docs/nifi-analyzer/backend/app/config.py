"""Application configuration via Pydantic BaseSettings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Global application settings loaded from environment variables."""

    CORS_ORIGINS: list[str] = ["http://localhost:5174", "http://127.0.0.1:5174"]
    LOG_LEVEL: str = "INFO"
    MAX_FILE_SIZE: int = 50 * 1024 * 1024  # 50 MB

    model_config = {"env_prefix": "ETL_", "case_sensitive": True}


settings = Settings()
