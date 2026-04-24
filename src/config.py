"""Pipeline configuration from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # SQL Server
    DB_SERVER: str   = os.getenv("DB_SERVER", "localhost")
    DB_NAME: str     = os.getenv("DB_NAME", "etl_staging")
    DB_USER: str     = os.getenv("DB_USER", "")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_DRIVER: str   = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    # SFTP
    SFTP_HOST: str       = os.getenv("SFTP_HOST", "")
    SFTP_PORT: int       = int(os.getenv("SFTP_PORT", "22"))
    SFTP_USER: str       = os.getenv("SFTP_USER", "")
    SFTP_PASSWORD: str   = os.getenv("SFTP_PASSWORD", "")
    SFTP_KEY_PATH: str   = os.getenv("SFTP_KEY_PATH", "")
    SFTP_REMOTE_DIR: str = os.getenv("SFTP_REMOTE_DIR", "/uploads/")

    # REST API
    API_BASE_URL: str = os.getenv("API_BASE_URL", "")
    API_KEY: str      = os.getenv("API_KEY", "")
    API_TIMEOUT: int  = int(os.getenv("API_TIMEOUT", "30"))

    # AS400
    AS400_LINKED_SERVER: str = os.getenv("AS400_LINKED_SERVER", "AS400_SERVER")
    AS400_LIBRARY: str       = os.getenv("AS400_LIBRARY", "YOURLIB")

    # Pipeline
    LOG_LEVEL: str  = os.getenv("LOG_LEVEL", "INFO")
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))

    @property
    def connection_string(self) -> str:
        if self.DB_USER:
            return (
                f"DRIVER={{{self.DB_DRIVER}}};"
                f"SERVER={self.DB_SERVER};"
                f"DATABASE={self.DB_NAME};"
                f"UID={self.DB_USER};PWD={self.DB_PASSWORD};"
            )
        return (
            f"DRIVER={{{self.DB_DRIVER}}};"
            f"SERVER={self.DB_SERVER};"
            f"DATABASE={self.DB_NAME};"
            "Trusted_Connection=yes;"
        )


config = Config()
