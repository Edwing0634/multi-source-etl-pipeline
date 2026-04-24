"""Extract data from AS400/iSeries via SQL Server OPENQUERY linked server."""

import logging
import pandas as pd
import pyodbc
from src.config import config

logger = logging.getLogger(__name__)


class AS400Extractor:
    """
    Reads AS400 data through a SQL Server linked server using OPENQUERY.
    The linked server must be pre-configured by the DBA.
    Falls back to demo data when the DB is not available.
    """

    def __init__(self) -> None:
        self._conn: pyodbc.Connection | None = None
        self.demo_mode: bool = not bool(config.DB_SERVER and config.DB_SERVER != "localhost")

    def connect(self) -> None:
        try:
            self._conn = pyodbc.connect(config.connection_string, timeout=10)
            logger.info("SQL Server connected for AS400 OPENQUERY")
            self.demo_mode = False
        except pyodbc.Error as exc:
            logger.warning("DB connection failed, using demo mode: %s", exc)
            self.demo_mode = True

    def extract(self, as400_table: str, where_clause: str = "") -> pd.DataFrame:
        """
        Executes OPENQUERY against the linked AS400 server.

        Args:
            as400_table:  Table name in the AS400 library (e.g. 'POLICIES')
            where_clause: Optional WHERE clause without the WHERE keyword
        """
        if self.demo_mode:
            logger.warning("AS400 demo mode — returning fake data")
            return self._demo_data(as400_table)

        where = f"WHERE {where_clause}" if where_clause else ""
        query = f"""
            SELECT *
            FROM OPENQUERY(
                {config.AS400_LINKED_SERVER},
                'SELECT * FROM {config.AS400_LIBRARY}.{as400_table} {where}'
            )
        """
        df = pd.read_sql(query, self._conn)
        logger.info("AS400 extracted: %s → %d rows", as400_table, len(df))
        return df

    def extract_with_columns(
        self,
        as400_table: str,
        columns: list[str],
        where_clause: str = "",
    ) -> pd.DataFrame:
        """Extract specific columns to avoid SELECT * over slow linked server."""
        if self.demo_mode:
            return self._demo_data(as400_table)[columns] if columns else self._demo_data(as400_table)

        col_list = ", ".join(columns)
        where = f"WHERE {where_clause}" if where_clause else ""
        query = f"""
            SELECT *
            FROM OPENQUERY(
                {config.AS400_LINKED_SERVER},
                'SELECT {col_list} FROM {config.AS400_LIBRARY}.{as400_table} {where}'
            )
        """
        df = pd.read_sql(query, self._conn)
        logger.info("AS400 extracted %d columns from %s: %d rows", len(columns), as400_table, len(df))
        return df

    def close(self) -> None:
        if self._conn:
            self._conn.close()

    @staticmethod
    def _demo_data(table_name: str) -> pd.DataFrame:
        import random
        rows = []
        for i in range(25):
            rows.append({
                "RECORD_ID":     i + 1,
                "POLICY_NUM":    f"AS4-{2024}-{i + 1:06d}",
                "CLIENT_CODE":   f"C{10000 + i}",
                "PREMIUM":       round(random.uniform(80_000, 600_000), 0),
                "ISSUE_DATE":    f"202{random.randint(1,4)}-{random.randint(1,12):02d}-01",
                "STATUS_CODE":   random.choice(["A", "L", "C", "P"]),
                "TABLE_SOURCE":  table_name,
            })
        return pd.DataFrame(rows)
