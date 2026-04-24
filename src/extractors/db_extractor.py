"""Extract data from SQL Server tables or arbitrary queries."""

import logging
import pandas as pd
import pyodbc
from src.config import config

logger = logging.getLogger(__name__)


class DBExtractor:
    """Reads data from SQL Server via pyodbc into DataFrames."""

    def __init__(self, connection_string: str = "") -> None:
        self._conn_str = connection_string or config.connection_string
        self._conn: pyodbc.Connection | None = None

    def connect(self) -> None:
        self._conn = pyodbc.connect(self._conn_str, timeout=10)
        logger.info("DBExtractor connected")

    def extract_table(self, table: str, schema: str = "dbo", where: str = "") -> pd.DataFrame:
        where_clause = f"WHERE {where}" if where else ""
        query = f"SELECT * FROM {schema}.{table} {where_clause}"
        return self.extract_query(query)

    def extract_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        if not self._conn:
            self.connect()
        df = pd.read_sql(query, self._conn, params=params if params else None)
        logger.info("DB query extracted: %d rows", len(df))
        return df

    def extract_stored_procedure(self, sp_name: str, params: dict | None = None) -> pd.DataFrame:
        """Execute a stored procedure and return its first result set."""
        if not self._conn:
            self.connect()
        param_str = ", ".join(f"@{k}=?" for k in (params or {}).keys())
        call = f"EXEC {sp_name} {param_str}"
        df = pd.read_sql(call, self._conn, params=list((params or {}).values()) or None)
        logger.info("SP %s extracted: %d rows", sp_name, len(df))
        return df

    def close(self) -> None:
        if self._conn:
            self._conn.close()
