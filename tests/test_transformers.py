"""Unit tests for Cleaner and Mapper transforms."""

import pandas as pd
import pytest
from src.transformers.cleaner import Cleaner
from src.transformers.mapper  import Mapper


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "  Name  ": ["  Alice  ", "Bob", "Alice"],
        "Status":   ["Activo", "inactive", "ACTIVE"],
        "Amount":   ["$1.500.000", "200000", None],
        "Flag":     ["Yes", "no", "si"],
    })


class TestCleaner:
    def test_normalize_columns(self, sample_df):
        df = Cleaner()._normalize_columns(sample_df)
        assert "name" in df.columns
        assert "status" in df.columns

    def test_strip_strings(self, sample_df):
        df = Cleaner()._normalize_columns(sample_df)
        df = Cleaner()._strip_strings(df)
        assert df["name"].iloc[0] == "Alice"

    def test_drop_duplicates(self, sample_df):
        df = Cleaner()._normalize_columns(sample_df)
        df = Cleaner()._strip_strings(df)
        before = len(df)
        df = Cleaner()._drop_full_duplicates(df)
        assert len(df) < before

    def test_coerce_numeric(self):
        df = pd.DataFrame({"amount": ["$1.500.000", "200000", "abc", None]})
        result = Cleaner().coerce_numeric(df, ["amount"])
        assert result["amount"].dtype in ["float64", "object"]

    def test_fill_nulls_string(self):
        df = pd.DataFrame({"name": ["Alice", None, "Bob"]})
        result = Cleaner().fill_nulls(df, strategy="empty_string")
        assert result["name"].isna().sum() == 0


class TestMapper:
    def test_rename_columns(self):
        df = pd.DataFrame({"old_name": [1, 2], "keep": [3, 4]})
        result = Mapper().rename_columns(df, {"old_name": "new_name"})
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_rename_missing_column_warns(self, caplog):
        df = pd.DataFrame({"a": [1]})
        import logging
        with caplog.at_level(logging.WARNING):
            Mapper().rename_columns(df, {"nonexistent": "x"})
        assert "not found" in caplog.text

    def test_map_values(self):
        df = pd.DataFrame({"color": ["red", "blue", "green"]})
        result = Mapper().map_values(df, "color", {"red": "rojo", "blue": "azul"})
        assert result["color"].iloc[0] == "rojo"
        assert result["color"].iloc[2] == "green"  # unmapped stays

    def test_add_audit_columns(self):
        df = pd.DataFrame({"id": [1, 2]})
        result = Mapper().add_audit_columns(df, source="test")
        assert "_etl_source" in result.columns
        assert "_etl_loaded_at" in result.columns
        assert (result["_etl_source"] == "test").all()

    def test_standardize_status(self):
        df = pd.DataFrame({"status": ["A", "activo", "L", "unknown_val"]})
        result = Mapper().standardize_status(df, "status")
        assert result["status"].iloc[0] == "active"
        assert result["status"].iloc[2] == "lapsed"
        assert result["status"].iloc[3] == "unknown"

    def test_select_and_order(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = Mapper().select_and_order(df, ["c", "a"])
        assert list(result.columns) == ["c", "a"]
