"""Extract data from local flat files: CSV, Excel, and delimited text."""

import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class FileExtractor:
    """Reads CSV, Excel, and pipe-delimited files into DataFrames."""

    def extract(self, file_path: str, **kwargs) -> pd.DataFrame:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        suffix = path.suffix.lower()
        readers = {
            ".csv":  self._read_csv,
            ".txt":  self._read_csv,
            ".xlsx": self._read_excel,
            ".xls":  self._read_excel,
        }
        reader = readers.get(suffix)
        if not reader:
            raise ValueError(f"Unsupported file type: {suffix}")

        df = reader(str(path), **kwargs)
        logger.info("File extracted: %s → %d rows × %d cols", path.name, len(df), len(df.columns))
        return df

    def extract_directory(self, dir_path: str, pattern: str = "*.csv") -> pd.DataFrame:
        """Read and concatenate all matching files in a directory."""
        files = list(Path(dir_path).glob(pattern))
        if not files:
            logger.warning("No files matched '%s' in %s", pattern, dir_path)
            return pd.DataFrame()

        frames = [self.extract(str(f)) for f in files]
        combined = pd.concat(frames, ignore_index=True)
        logger.info("Directory extracted: %d rows from %d file(s)", len(combined), len(files))
        return combined

    def _read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        defaults = {"encoding": "utf-8-sig", "low_memory": False}
        defaults.update(kwargs)
        # Try comma first, then semicolon, then pipe
        for sep in (",", ";", "|", "\t"):
            try:
                df = pd.read_csv(path, sep=sep, **defaults)
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
        return pd.read_csv(path, **defaults)

    def _read_excel(self, path: str, **kwargs) -> pd.DataFrame:
        defaults = {"engine": "openpyxl"}
        defaults.update(kwargs)
        return pd.read_excel(path, **defaults)
