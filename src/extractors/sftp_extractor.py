"""Extract CSV/Excel files from a remote SFTP server."""

import io
import logging
from pathlib import Path
import pandas as pd
import paramiko
from src.config import config

logger = logging.getLogger(__name__)


class SFTPExtractor:
    """Downloads and parses files from a remote SFTP directory."""

    def __init__(self) -> None:
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def connect(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict = {
            "hostname": config.SFTP_HOST,
            "port": config.SFTP_PORT,
            "username": config.SFTP_USER,
        }
        if config.SFTP_KEY_PATH:
            connect_kwargs["key_filename"] = config.SFTP_KEY_PATH
        else:
            connect_kwargs["password"] = config.SFTP_PASSWORD

        self._client.connect(**connect_kwargs)
        self._sftp = self._client.open_sftp()
        logger.info("SFTP connected: %s", config.SFTP_HOST)

    def disconnect(self) -> None:
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()
        logger.info("SFTP disconnected")

    def list_files(self, pattern: str = "") -> list[str]:
        """Return filenames in the remote directory, optionally filtered by pattern."""
        if not self._sftp:
            raise RuntimeError("Not connected — call connect() first")
        files = self._sftp.listdir(config.SFTP_REMOTE_DIR)
        if pattern:
            files = [f for f in files if pattern in f]
        logger.info("SFTP: %d file(s) found in %s", len(files), config.SFTP_REMOTE_DIR)
        return files

    def extract(self, filename: str) -> pd.DataFrame:
        """Download a single file and return it as a DataFrame."""
        if not self._sftp:
            raise RuntimeError("Not connected — call connect() first")

        remote_path = f"{config.SFTP_REMOTE_DIR}{filename}"
        buf = io.BytesIO()
        self._sftp.getfo(remote_path, buf)
        buf.seek(0)

        suffix = Path(filename).suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(buf, encoding="utf-8-sig")
        elif suffix in (".xlsx", ".xls"):
            df = pd.read_excel(buf)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        logger.info("SFTP extracted: %s → %d rows", filename, len(df))
        return df

    def extract_all(self, pattern: str = "") -> pd.DataFrame:
        """Download and concatenate all matching files from the remote directory."""
        files = self.list_files(pattern)
        if not files:
            logger.warning("No files matched pattern '%s'", pattern)
            return pd.DataFrame()

        frames = [self.extract(f) for f in files]
        combined = pd.concat(frames, ignore_index=True)
        logger.info("SFTP total extracted: %d rows from %d file(s)", len(combined), len(files))
        return combined

    # Demo helper — returns fake data when SFTP is not configured
    @staticmethod
    def demo_data() -> pd.DataFrame:
        import random
        from datetime import date, timedelta
        rows = []
        for i in range(50):
            rows.append({
                "record_id": i + 1,
                "client_code": f"CLI{1000 + i:04d}",
                "policy_number": f"POL-{2024}-{i + 1:05d}",
                "premium_amount": round(random.uniform(50_000, 500_000), 2),
                "status": random.choice(["active", "lapsed", "pending"]),
                "effective_date": str(date.today() - timedelta(days=random.randint(0, 365))),
                "source": "sftp",
            })
        return pd.DataFrame(rows)
