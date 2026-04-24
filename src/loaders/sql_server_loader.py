"""Load DataFrames into SQL Server using BULK INSERT and MERGE patterns."""

import logging
from typing import Literal
import pandas as pd
import pyodbc
from src.config import config

logger = logging.getLogger(__name__)


class SQLServerLoader:
    """Writes DataFrames to SQL Server staging tables with MERGE dedup support."""

    def __init__(self) -> None:
        self._conn: pyodbc.Connection | None = None

    def connect(self) -> None:
        self._conn = pyodbc.connect(config.connection_string, autocommit=False)
        logger.info("SQLServerLoader connected")

    def _cursor(self) -> pyodbc.Cursor:
        if not self._conn:
            self.connect()
        return self._conn.cursor()

    def load(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "staging",
        mode: Literal["append", "replace", "merge"] = "append",
        merge_key: str | None = None,
        batch_size: int | None = None,
    ) -> int:
        """
        Load a DataFrame into SQL Server.

        Args:
            df:         Source DataFrame
            table:      Target table name
            schema:     Target schema (default: staging)
            mode:       'append' | 'replace' | 'merge'
            merge_key:  Column used as unique key for MERGE (required when mode='merge')
            batch_size: Rows per batch (defaults to config.BATCH_SIZE)
        """
        if df.empty:
            logger.warning("Empty DataFrame — nothing to load into %s.%s", schema, table)
            return 0

        batch_size = batch_size or config.BATCH_SIZE

        if mode == "replace":
            self._truncate(schema, table)

        if mode == "merge" and merge_key:
            return self._merge(df, schema, table, merge_key, batch_size)

        return self._bulk_insert(df, schema, table, batch_size)

    def _truncate(self, schema: str, table: str) -> None:
        cur = self._cursor()
        cur.execute(f"TRUNCATE TABLE {schema}.{table}")
        self._conn.commit()
        logger.info("Truncated %s.%s", schema, table)

    def _bulk_insert(self, df: pd.DataFrame, schema: str, table: str, batch_size: int) -> int:
        cur = self._cursor()
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        sql = f"INSERT INTO {schema}.{table} ({cols}) VALUES ({placeholders})"

        total = 0
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i: i + batch_size]
            rows  = [tuple(row) for row in batch.itertuples(index=False, name=None)]
            cur.executemany(sql, rows)
            self._conn.commit()
            total += len(rows)
            logger.debug("Inserted batch %d–%d (%d rows)", i, i + len(rows), len(rows))

        logger.info("Loaded %d rows → %s.%s", total, schema, table)
        return total

    def _merge(self, df: pd.DataFrame, schema: str, table: str, key: str, batch_size: int) -> int:
        """
        MERGE pattern: upsert rows using a temporary staging table.
        INSERT new rows, UPDATE existing rows matched on key column.
        """
        tmp_table = f"##etl_tmp_{table}"
        cur = self._cursor()

        # Create temp table with same structure
        cur.execute(f"SELECT TOP 0 * INTO {tmp_table} FROM {schema}.{table}")
        self._conn.commit()

        # Load into temp
        self._bulk_insert(df, schema=tmp_table.replace("##", "##"), table="", batch_size=batch_size)

        non_key_cols = [c for c in df.columns if c != key]
        update_set   = ", ".join(f"t.{c} = s.{c}" for c in non_key_cols)
        insert_cols  = ", ".join(df.columns)
        insert_vals  = ", ".join(f"s.{c}" for c in df.columns)

        merge_sql = f"""
            MERGE {schema}.{table} AS t
            USING {tmp_table} AS s
               ON t.{key} = s.{key}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals});
        """
        cur.execute(merge_sql)
        affected = cur.rowcount
        self._conn.commit()
        cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        self._conn.commit()

        logger.info("MERGE complete: %d rows affected → %s.%s", affected, schema, table)
        return affected

    def execute_sp(self, sp_name: str, params: dict | None = None) -> None:
        """Execute a post-load stored procedure (e.g. sp_merge_staging)."""
        cur = self._cursor()
        param_str = ", ".join(f"@{k}=?" for k in (params or {}).keys())
        cur.execute(f"EXEC {sp_name} {param_str}", list((params or {}).values()))
        self._conn.commit()
        logger.info("Executed SP: %s", sp_name)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
