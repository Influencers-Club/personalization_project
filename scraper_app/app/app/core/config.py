import os
from typing import Any, Dict, Optional, List
from pydantic import BaseSettings, validator, AnyHttpUrl


def create_append_dir(target_dir="", append_dir=""):
    if target_dir:

        result_dir = target_dir
        if append_dir:
            result_dir = os.path.join(target_dir, append_dir)

        if not os.path.exists(result_dir):
            os.makedirs(result_dir)

        return result_dir

    return ""


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Scraper_Template_Project"
    PRINT_INTERVAL_MIN: Optional[int] = 5

    MONGO_SERVER: Optional[str] = None
    MONGO_PORT: Optional[int] = None
    MONGO_DATABASE: Optional[str] = None
    MONGO_TABLE: Optional[str] = None
    MONGO_USERNAME: Optional[str] = None
    MONGO_PASSWORD: Optional[str] = None
    MONGO_DATABASE_URI: Optional[Any] = None

    POSTGRES_SERVER: Optional[str] = None
    POSTGRES_USER: Optional[str] = None
    POSTGRES_PASSWORD: Optional[str] = None
    POSTGRES_DB: Optional[str] = None
    SQLALCHEMY_DATABASE_URI: Optional[Any] = None

    OUTPUT_DIR: Optional[str] = ""
    ERROR_DIR: Optional[str] = ""
    INPUT_DIR: Optional[str] = ""
    LOG_DIR: Optional[str] = ""
    EXPORT_DIR: Optional[str] = ""

    KAFKA_HOST: Optional[str] = ""
    KAFKA_PORT: Optional[str] = ""

    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_BUCKET: Optional[str] = None

    KAFKA_USERNAME: Optional[str] = ""
    KAFKA_PASSWORD: Optional[str] = ""
    KAFKA_SERVER: Optional[str] = ""

    PROXY_MANAGEMENT_SYSTEM_URL: Optional[str] = ""

    @validator("ERROR_DIR", pre=True)
    def assemble_error_dir(cls, v: Optional[str], values: Dict[str, Any]) -> Any:

        if v and isinstance(v, str):
            return create_append_dir(target_dir=v)

        return create_append_dir(target_dir=values.get("OUTPUT_DIR"), append_dir="errors")

    @validator("INPUT_DIR", pre=True)
    def assemble_input_dir(cls, v: Optional[str], values: Dict[str, Any]) -> Any:

        if v and isinstance(v, str):
            return create_append_dir(target_dir=v)

        return create_append_dir(target_dir=values.get("OUTPUT_DIR"), append_dir="inputs")

    @validator("LOG_DIR", pre=True)
    def assemble_log_dir(cls, v: Optional[str], values: Dict[str, Any]) -> Any:

        if v and isinstance(v, str):
            return create_append_dir(target_dir=v)

        return create_append_dir(target_dir=values.get("OUTPUT_DIR"), append_dir="logs")

    @validator("EXPORT_DIR", pre=True)
    def assemble_export_dir(cls, v: Optional[str], values: Dict[str, Any]) -> Any:

        if v and isinstance(v, str):
            return create_append_dir(target_dir=v)

        return create_append_dir(target_dir=values.get("OUTPUT_DIR"), append_dir="export")

    CELERY_BROKER_URL: Optional[str] = ""
    CELERY_RESULT_BACKEND: Optional[str] = ""
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    USE_LOCAL: Optional[bool] = False

    class Config:
        case_sensitive = True
        env_file = ".env"


settings = Settings()
