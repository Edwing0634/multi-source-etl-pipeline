"""Unit tests for extractor demo data and file parsing."""

import pandas as pd
import pytest
from src.extractors.sftp_extractor import SFTPExtractor
from src.extractors.api_extractor  import APIExtractor
from src.extractors.as400_extractor import AS400Extractor


class TestSFTPExtractor:
    def test_demo_data_shape(self):
        df = SFTPExtractor.demo_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 50
        assert "policy_number" in df.columns
        assert "premium_amount" in df.columns

    def test_demo_data_no_nulls_in_key(self):
        df = SFTPExtractor.demo_data()
        assert df["policy_number"].notna().all()

    def test_demo_data_status_values(self):
        df = SFTPExtractor.demo_data()
        valid = {"active", "lapsed", "pending"}
        assert set(df["status"].unique()).issubset(valid)


class TestAPIExtractor:
    def test_demo_survey_data_shape(self):
        df = APIExtractor.demo_survey_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 30
        assert "response_id" in df.columns

    def test_demo_survey_scores_range(self):
        df = APIExtractor.demo_survey_data()
        assert df["q1_satisfaction"].between(1, 5).all()
        assert df["q2_recommend"].between(0, 10).all()

    def test_demo_sms_data_shape(self):
        df = APIExtractor.demo_sms_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 40
        assert "status" in df.columns

    def test_demo_sms_status_values(self):
        df = APIExtractor.demo_sms_data()
        valid = {"DELIVERED", "UNDELIVERABLE", "PENDING"}
        assert set(df["status"].unique()).issubset(valid)


class TestAS400Extractor:
    def test_demo_data_shape(self):
        df = AS400Extractor._demo_data("POLICIES")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 25
        assert "POLICY_NUM" in df.columns

    def test_demo_data_table_source(self):
        df = AS400Extractor._demo_data("CLIENTS")
        assert (df["TABLE_SOURCE"] == "CLIENTS").all()
