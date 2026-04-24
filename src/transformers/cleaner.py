"""DataFrame cleaning and standardization transforms."""

import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)


class Cleaner:
    """Applies a standard cleaning pipeline to raw DataFrames."""

    def clean(self, df: pd.DataFrame, source: str = "") -> pd.DataFrame:
        original_len = len(df)
        df = (
            df
            .pipe(self._normalize_columns)
            .pipe(self._strip_strings)
            .pipe(self._drop_full_duplicates)
            .pipe(self._normalize_booleans)
        )
        logger.info(
            "[%s] Cleaned: %d → %d rows (-%d duplicates/blanks)",
            source or "unknown", original_len, len(df), original_len - len(df),
        )
        return df

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [
            re.sub(r"[^a-z0-9_]", "_", col.strip().lower())
            for col in df.columns
        ]
        return df

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
        return df

    def _drop_full_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates()
        dropped = before - len(df)
        if dropped:
            logger.debug("Dropped %d duplicate rows", dropped)
        return df

    def _normalize_booleans(self, df: pd.DataFrame) -> pd.DataFrame:
        true_vals  = {"yes", "y", "si", "sí", "true", "1", "x", "activo", "active"}
        false_vals = {"no", "n", "false", "0", "", "inactivo", "inactive"}
        for col in df.select_dtypes(include="object").columns:
            sample = df[col].dropna().str.lower().unique()
            if set(sample).issubset(true_vals | false_vals):
                df[col] = df[col].str.lower().map(
                    lambda v: True if v in true_vals else (False if v in false_vals else None)
                )
        return df

    def coerce_dates(self, df: pd.DataFrame, columns: list[str], fmt: str = "mixed") -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
        return df

    def coerce_numeric(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r"[$,.](?=\d{3})", "", regex=True),
                    errors="coerce",
                )
        return df

    def fill_nulls(self, df: pd.DataFrame, strategy: str = "empty_string") -> pd.DataFrame:
        if strategy == "empty_string":
            str_cols = df.select_dtypes(include="object").columns
            df[str_cols] = df[str_cols].fillna("")
        elif strategy == "zero":
            num_cols = df.select_dtypes(include="number").columns
            df[num_cols] = df[num_cols].fillna(0)
        return df
