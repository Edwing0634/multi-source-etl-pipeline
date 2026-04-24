# Source Mapping Reference

## SFTP Source

| Source Column | Staging Column | Type | Notes |
|--------------|----------------|------|-------|
| `record_id` | `record_id` | INT | Source PK |
| `client_code` | `client_code` | VARCHAR(20) | Client identifier |
| `policy_number` | `policy_number` | VARCHAR(30) | **Merge key** |
| `premium_amount` | `premium_amount` | DECIMAL(18,2) | Cleaned of $ and separators |
| `status` | `status` | VARCHAR(20) | Normalized: active/lapsed/pending/canceled |
| `effective_date` | `effective_date` | DATE | Coerced from mixed formats |

## Survey API Source

| API Field | Staging Column | Type | Notes |
|-----------|----------------|------|-------|
| `responseId` | `response_id` | VARCHAR(20) | **Merge key** |
| `recordedDate` | `recorded_date` | DATE | |
| `Q1` | `q1_satisfaction` | TINYINT | 1–5 scale |
| `Q2` | `q2_recommend` | TINYINT | 0–10 NPS scale |
| `Q3_TEXT` | `q3_comment` | NVARCHAR(500) | Free text |

## AS400 Source

| AS400 Field | Staging Column | Type | Notes |
|-------------|----------------|------|-------|
| `RECORD_ID` | `record_id` | INT | |
| `POLICY_NUM` | `policy_num` | VARCHAR(30) | **Merge key** |
| `CLIENT_CODE` | `client_code` | VARCHAR(20) | |
| `PREMIUM` | `premium` | DECIMAL(18,2) | |
| `ISSUE_DATE` | `issue_date` | DATE | |
| `STATUS_CODE` | `status_code` | VARCHAR(20) | A=active L=lapsed C=canceled |

## Status Code Normalization

| Raw Value | Normalized |
|-----------|-----------|
| `A`, `activo`, `active`, `1` | `active` |
| `L`, `vencido`, `lapsed` | `lapsed` |
| `C`, `cancelado`, `canceled` | `canceled` |
| `P`, `pendiente`, `pending` | `pending` |
| `E`, `expirado`, `expired` | `expired` |
| anything else | `unknown` |
