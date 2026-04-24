# 🔄 Multi-Source ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL%20Server-pyodbc-CC2927?logo=microsoftsqlserver)
![License](https://img.shields.io/badge/License-MIT-green)
![Last Commit](https://img.shields.io/github/last-commit/Edwing0634/multi-source-etl-pipeline)

> Production-grade ETL pipeline that extracts data from **5+ heterogeneous sources** — SFTP, REST APIs, AS400/OPENQUERY, flat files, and database-to-database — cleans and standardizes it, then loads into SQL Server using BULK INSERT and MERGE patterns.

---

## 🏭 Background

Built from experience maintaining a **daily data integration layer at a multinational insurance company in Colombia**, where data arrives from legacy AS400 systems, third-party SMS/survey platforms via REST APIs, SFTP file drops, and internal SQL Server databases — all needing to land in a unified reporting schema before the business day starts.

The 9-script cascade pattern, MERGE upsert logic, and isolated job scheduling were designed for exactly this kind of environment: heterogeneous sources, no tolerance for one broken feed taking down the rest, and a non-technical ops team that needs clear run logs to diagnose issues.

---

## ✨ Features

- **5 extractor types** — SFTP (paramiko), REST APIs (paginated + retry), AS400 via OPENQUERY linked server, local CSV/Excel files, and direct DB queries
- **Modular transform layer** — column normalization, string cleaning, deduplication, status normalization, type coercion, audit columns
- **3 load strategies** — `append`, `replace` (truncate+insert), and `merge` (upsert via temp table + MERGE statement)
- **Stored procedures** — `sp_merge_staging` (parameterized MERGE) and `sp_update_timestamps` (run logging)
- **Cron scheduler** — built-in `schedule`-based runner; each job is isolated so one failure doesn't stop others
- **Demo mode** — all extractors fall back to realistic fake data when connections aren't configured
- **Full test suite** — pytest tests for extractors and transformers using only demo data (no DB needed)

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| SFTP extraction | `paramiko` |
| REST API | `requests` (retry + pagination) |
| AS400 | SQL Server OPENQUERY linked server |
| File parsing | `pandas` + `openpyxl` |
| Database | `pyodbc` → SQL Server |
| Scheduling | `schedule` |
| Testing | `pytest` |

---

## ⚡ Quick Start

```bash
# 1. Clone and install
git clone https://github.com/Edwing0634/multi-source-etl-pipeline.git
cd multi-source-etl-pipeline
pip install -r requirements.txt

# 2. Configure (optional — demo mode works without any config)
cp .env.example .env

# 3. Run full pipeline (demo mode)
python -m src.orchestrator

# 4. Run a single job
python -m src.orchestrator --job sftp
python -m src.orchestrator --job api_survey
python -m src.orchestrator --job as400
python -m src.orchestrator --job sms_report

# 5. Run on schedule (daily + hourly)
python -m src.orchestrator --schedule

# 6. Run tests
pytest tests/ -v
```

---

## 📁 Project Structure

```
multi-source-etl-pipeline/
├── src/
│   ├── config.py                   # Env-based configuration
│   ├── orchestrator.py             # Job runner + scheduler
│   ├── extractors/
│   │   ├── sftp_extractor.py       # paramiko SFTP + CSV/Excel parsing
│   │   ├── api_extractor.py        # REST API + pagination + retry
│   │   ├── as400_extractor.py      # OPENQUERY via linked server
│   │   ├── file_extractor.py       # Local CSV/Excel directory scanner
│   │   └── db_extractor.py         # Direct SQL Server queries + SPs
│   ├── transformers/
│   │   ├── cleaner.py              # Normalize, strip, dedup, coerce
│   │   └── mapper.py               # Rename, map values, audit cols
│   └── loaders/
│       └── sql_server_loader.py    # BULK INSERT + MERGE + TRUNCATE
├── sql/
│   ├── schemas/staging_tables.sql  # CREATE TABLE statements
│   └── stored_procedures/
│       ├── sp_merge_staging.sql    # Parameterized MERGE
│       └── sp_update_timestamps.sql # ETL run logging
├── tests/
│   ├── test_extractors.py
│   └── test_transformers.py
└── docs/
    ├── data_flow_diagram.md
    └── source_mapping.md
```

---

## 🏗 Architecture

```
Sources           Extract           Transform         Load
───────           ───────           ─────────         ────
SFTP         →   SFTPExtractor  →  Cleaner       →   SQLServerLoader
REST API     →   APIExtractor   →  Mapper        →   (append / merge)
AS400        →   AS400Extractor →               →   SQL Server staging
Flat files   →   FileExtractor  →               →
SQL Server   →   DBExtractor    →               →
                      │
                 Orchestrator (schedule / CLI)
```

---

## ⚙️ Configuration

| Variable | Description |
|----------|-------------|
| `DB_SERVER` | SQL Server hostname |
| `DB_NAME` | Target database |
| `SFTP_HOST` | SFTP server hostname |
| `SFTP_USER` / `SFTP_PASSWORD` | SFTP credentials |
| `SFTP_KEY_PATH` | Path to private key (optional) |
| `API_BASE_URL` | REST API base URL |
| `API_KEY` | API authentication key |
| `AS400_LINKED_SERVER` | SQL Server linked server name |
| `BATCH_SIZE` | Rows per insert batch (default: 1000) |

---

## 🗄 MERGE Pattern

```sql
MERGE staging.sftp_policies AS target
USING ##etl_tmp_sftp_policies AS source
   ON source.policy_number = target.policy_number
WHEN MATCHED THEN
    UPDATE SET target._etl_loaded_at = source._etl_loaded_at
WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...);
```

---

## 🤝 Contributing

1. Fork the repository
2. Add a new extractor in `src/extractors/` following the existing pattern
3. Include a `demo_data()` static method for offline testing
4. Add tests in `tests/test_extractors.py`
5. Open a Pull Request

---

## 📄 License

MIT © [Edwin González](https://github.com/Edwing0634)

---

## 👤 Author

**Edwin González** — Data Engineer & Python Developer

[![GitHub](https://img.shields.io/badge/GitHub-Edwing0634-black?logo=github)](https://github.com/Edwing0634)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://linkedin.com/in/edwingonzalez)
