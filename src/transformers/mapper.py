"""Column renaming, type casting, and value mapping transforms."""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Mapper:
    """Applies column mapping and value normalization to DataFrames."""

    def rename_columns(self, df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        existing = {k: v for k, v in mapping.items() if k in df.columns}
        df = df.rename(columns=existing)
        missing = set(mapping.keys()) - set(existing.keys())
        if missing:
            logger.warning("Columns not found for renaming: %s", missing)
        return df

    def map_values(self, df: pd.DataFrame, column: str, mapping: dict) -> pd.DataFrame:
        if column not in df.columns:
            logger.warning("Column '%s' not found for value mapping", column)
            return df
        df[column] = df[column].map(mapping).fillna(df[column])
        return df

    def add_audit_columns(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """Add ETL audit columns for traceability."""
        from datetime import datetime, timezone
        df["_etl_source"]     = source
        df["_etl_loaded_at"]  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return df

    def select_and_order(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Select only the specified columns in the given order."""
        available = [c for c in columns if c in df.columns]
        missing = set(columns) - set(available)
        if missing:
            logger.warning("Columns not available for selection: %s", missing)
        return df[available]

    def standardize_status(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Normalize freeform status values to a standard vocabulary."""
        status_map = {
            "a": "active",   "activo": "active",   "active": "active",    "1": "active",
            "l": "lapsed",   "vencido": "lapsed",  "lapsed": "lapsed",
            "c": "canceled", "cancelado": "canceled", "canceled": "canceled",
            "p": "pending",  "pendiente": "pending",  "pending": "pending",
            "e": "expired",  "expirado": "expired",   "expired": "expired",
        }
        if column in df.columns:
            df[column] = df[column].astype(str).str.lower().map(status_map).fillna("unknown")
        return df
