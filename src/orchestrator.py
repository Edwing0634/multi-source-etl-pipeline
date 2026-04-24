"""
Pipeline orchestrator — runs all ETL jobs sequentially or on a schedule.

Usage:
    python -m src.orchestrator              # run once (all jobs)
    python -m src.orchestrator --job sftp   # run a single job
    python -m src.orchestrator --schedule   # run on cron schedule
"""

import argparse
import logging
import sys
import schedule
import time
from datetime import datetime

from src.extractors.sftp_extractor  import SFTPExtractor
from src.extractors.api_extractor   import APIExtractor
from src.extractors.as400_extractor import AS400Extractor
from src.extractors.file_extractor  import FileExtractor
from src.transformers.cleaner       import Cleaner
from src.transformers.mapper        import Mapper
from src.loaders.sql_server_loader  import SQLServerLoader
from src.config import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

cleaner = Cleaner()
mapper  = Mapper()
loader  = SQLServerLoader()


# ------------------------------------------------------------------
# Individual job definitions
# ------------------------------------------------------------------

def job_sftp() -> None:
    logger.info("=== JOB: SFTP Extraction ===")
    extractor = SFTPExtractor()
    try:
        extractor.connect()
        df = extractor.extract_all(pattern=".csv")
    except Exception:
        logger.warning("SFTP unavailable — using demo data")
        df = SFTPExtractor.demo_data()
    finally:
        extractor.disconnect()

    df = cleaner.clean(df, source="sftp")
    df = mapper.standardize_status(df, "status")
    df = mapper.add_audit_columns(df, source="sftp")
    loader.load(df, table="sftp_policies", mode="merge", merge_key="policy_number")
    logger.info("SFTP job complete: %d rows", len(df))


def job_api_survey() -> None:
    logger.info("=== JOB: Survey API Extraction ===")
    extractor = APIExtractor()
    df = extractor.extract_survey_responses(survey_id="SV_DEMO_001")
    df = cleaner.clean(df, source="api_survey")
    df = mapper.add_audit_columns(df, source="api_survey")
    loader.load(df, table="survey_responses", mode="append")
    logger.info("Survey API job complete: %d rows", len(df))


def job_as400() -> None:
    logger.info("=== JOB: AS400 Extraction ===")
    extractor = AS400Extractor()
    extractor.connect()
    df = extractor.extract("POLICIES")
    extractor.close()
    df = cleaner.clean(df, source="as400")
    df = mapper.standardize_status(df, "STATUS_CODE")
    df = mapper.add_audit_columns(df, source="as400")
    loader.load(df, table="as400_policies", mode="merge", merge_key="POLICY_NUM")
    logger.info("AS400 job complete: %d rows", len(df))


def job_sms_report() -> None:
    logger.info("=== JOB: SMS Delivery Report ===")
    extractor = APIExtractor()
    df = extractor.extract_sms_delivery_report(bulk_id="BULK_DEMO_001")
    df = cleaner.clean(df, source="api_sms")
    df = mapper.add_audit_columns(df, source="api_sms")
    loader.load(df, table="sms_delivery_log", mode="append")
    logger.info("SMS report job complete: %d rows", len(df))


ALL_JOBS: dict[str, callable] = {
    "sftp":       job_sftp,
    "api_survey": job_api_survey,
    "as400":      job_as400,
    "sms_report": job_sms_report,
}


def run_all() -> None:
    logger.info("Starting full pipeline run — %s", datetime.now().isoformat())
    loader.connect()
    for name, job in ALL_JOBS.items():
        try:
            job()
        except Exception as exc:
            logger.error("Job '%s' failed: %s", name, exc, exc_info=True)
    loader.close()
    logger.info("Pipeline run complete — %s", datetime.now().isoformat())


def run_scheduled() -> None:
    schedule.every().day.at("02:00").do(run_all)
    schedule.every().hour.at(":30").do(job_sms_report)
    logger.info("Scheduler started — waiting for jobs...")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pipeline Orchestrator")
    parser.add_argument("--job", choices=list(ALL_JOBS.keys()), help="Run a single job")
    parser.add_argument("--schedule", action="store_true", help="Run on cron schedule")
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    elif args.job:
        loader.connect()
        ALL_JOBS[args.job]()
        loader.close()
    else:
        run_all()
