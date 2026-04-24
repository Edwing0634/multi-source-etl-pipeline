# Data Flow Diagram

## End-to-End Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │   SFTP   │  │ REST API │  │  AS400   │  │  Flat Files  │  │
│  │CSV/Excel │  │ Survey / │  │OPENQUERY │  │  CSV/Excel   │  │
│  │  drops   │  │   SMS    │  │  linked  │  │  local dir   │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │
└───────┼─────────────┼─────────────┼────────────────┼──────────┘
        │             │             │                │
        └─────────────┴─────────────┴────────────────┘
                              │
                              ▼
                   ┌──────────────────┐
                   │   EXTRACT layer  │
                   │  sftp_extractor  │
                   │  api_extractor   │
                   │  as400_extractor │
                   │  file_extractor  │
                   │  db_extractor    │
                   └────────┬─────────┘
                            │  raw pd.DataFrame
                            ▼
                   ┌──────────────────┐
                   │ TRANSFORM layer  │
                   │  Cleaner         │  normalize cols, strip,
                   │                  │  dedup, coerce types
                   │  Mapper          │  rename, status normalize,
                   │                  │  audit columns
                   └────────┬─────────┘
                            │  clean pd.DataFrame
                            ▼
                   ┌──────────────────┐
                   │   LOAD layer     │
                   │  SQLServerLoader │  BULK INSERT (append)
                   │                  │  MERGE (upsert)
                   │                  │  TRUNCATE+INSERT (replace)
                   └────────┬─────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │     SQL Server              │
              │  schema: staging            │
              │  ├── sftp_policies          │
              │  ├── survey_responses       │
              │  ├── as400_policies         │
              │  └── sms_delivery_log       │
              │                             │
              │  schema: dbo               │
              │  └── etl_run_log           │
              └─────────────────────────────┘
```

## Job Schedule

| Job | Trigger | Source | Target Table | Mode |
|-----|---------|--------|-------------|------|
| `job_sftp` | Daily 02:00 | SFTP CSV drops | `staging.sftp_policies` | MERGE |
| `job_api_survey` | Daily 02:00 | REST API (Survey) | `staging.survey_responses` | APPEND |
| `job_as400` | Daily 02:00 | AS400 OPENQUERY | `staging.as400_policies` | MERGE |
| `job_sms_report` | Hourly :30 | REST API (SMS) | `staging.sms_delivery_log` | APPEND |

## Error Handling

- Each job runs independently — one failure does not stop others
- Errors are logged with full traceback via Python `logging`
- Demo mode activates automatically when connections fail
- All runs logged to `dbo.etl_run_log`
