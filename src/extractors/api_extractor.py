"""Extract data from REST APIs with pagination and retry support."""

import logging
import time
from typing import Any
import requests
from src.config import config

logger = logging.getLogger(__name__)

import pandas as pd


class APIExtractor:
    """Generic REST API extractor with pagination, retry, and auth headers."""

    def __init__(self, base_url: str = "", api_key: str = "") -> None:
        self._base_url = base_url or config.API_BASE_URL
        self._api_key  = api_key  or config.API_KEY
        self._session  = requests.Session()
        self._session.headers.update({
            "Authorization": f"App {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def get(self, endpoint: str, params: dict | None = None, retries: int = 3) -> dict[str, Any]:
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        for attempt in range(1, retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=config.API_TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                logger.warning("API attempt %d/%d failed: %s", attempt, retries, exc)
                if attempt < retries:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"API call failed after {retries} attempts: {url}")

    def extract_paginated(
        self,
        endpoint: str,
        page_param: str = "page",
        size_param: str = "pageSize",
        page_size: int = 100,
        results_key: str = "results",
    ) -> pd.DataFrame:
        """Fetch all pages from a paginated endpoint and return a combined DataFrame."""
        all_rows: list[dict] = []
        page = 1

        while True:
            params = {page_param: page, size_param: page_size}
            data = self.get(endpoint, params=params)
            rows = data.get(results_key, [])
            if not rows:
                break
            all_rows.extend(rows)
            logger.info("API page %d: %d records (total so far: %d)", page, len(rows), len(all_rows))
            if len(rows) < page_size:
                break
            page += 1

        df = pd.DataFrame(all_rows)
        logger.info("API extraction complete: %d rows", len(df))
        return df

    # ------------------------------------------------------------------
    # Vendor-specific helpers
    # ------------------------------------------------------------------

    def extract_survey_responses(self, survey_id: str) -> pd.DataFrame:
        """Qualtrics-style survey response extraction."""
        if not self._base_url:
            return self.demo_survey_data()
        data = self.get(f"surveys/{survey_id}/responses")
        rows = data.get("result", {}).get("elements", [])
        return pd.DataFrame(rows)

    def extract_sms_delivery_report(self, bulk_id: str) -> pd.DataFrame:
        """Infobip-style SMS delivery report extraction."""
        if not self._base_url:
            return self.demo_sms_data()
        data = self.get(f"sms/1/reports?bulkId={bulk_id}")
        rows = data.get("results", [])
        return pd.DataFrame(rows)

    # Demo helpers
    @staticmethod
    def demo_survey_data() -> pd.DataFrame:
        import random
        rows = []
        for i in range(30):
            rows.append({
                "response_id": f"R_{i+1:04d}",
                "recorded_date": f"2024-{(i % 12) + 1:02d}-15",
                "q1_satisfaction": random.randint(1, 5),
                "q2_recommend": random.randint(0, 10),
                "q3_comment": random.choice([
                    "Great service", "Could be better", "Very satisfied", "Fast response", ""
                ]),
                "source": "api_survey",
            })
        return pd.DataFrame(rows)

    @staticmethod
    def demo_sms_data() -> pd.DataFrame:
        import random
        rows = []
        for i in range(40):
            rows.append({
                "message_id": f"MSG{i+1:06d}",
                "to": f"57300{random.randint(1000000, 9999999)}",
                "sent_at": f"2024-06-{(i % 28) + 1:02d}T10:00:00",
                "status": random.choice(["DELIVERED", "DELIVERED", "DELIVERED", "UNDELIVERABLE", "PENDING"]),
                "error_code": None,
                "source": "api_sms",
            })
        return pd.DataFrame(rows)
