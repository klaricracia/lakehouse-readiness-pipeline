# Lakehouse Readiness Audit Report

Pre-build data readiness audit over every raw batch in `data/raw/`, run before any batch is admitted to the quality gate or merged into the lakehouse table. Verdicts are computed against the declared schema in `audit/readiness_audit.py::EXPECTED_SCHEMA`.

## Summary

- Batches scanned: **4**
- Passed: **3**
- Blocked: **1**

| Batch | Rows | Verdict | Blocking Issues |
|---|---|---|---|
| batch_2024_01.csv | 300 | **PASS** | — |
| batch_2024_02.csv | 315 | **PASS** | — |
| batch_2024_03.csv | 215 | **BLOCKED** | 4 issue(s) |
| batch_2024_04.csv | 265 | **PASS** | — |

## batch_2024_01.csv — PASS

- Rows: 300
- Columns: `record_id, submitted_at, status, region, processing_hours, assigned_office`

**Null rate per column:**

| Column | Null % |
|---|---|
| record_id | 0.0% |
| submitted_at | 0.0% |
| status | 0.0% |
| region | 0.0% |
| processing_hours | 0.0% |
| assigned_office | 0.0% |

No blocking issues found. Batch is clean for the quality gate.

## batch_2024_02.csv — PASS

- Rows: 315
- Columns: `record_id, submitted_at, status, region, processing_hours, assigned_office`

**Null rate per column:**

| Column | Null % |
|---|---|
| record_id | 0.0% |
| submitted_at | 0.0% |
| status | 0.0% |
| region | 0.0% |
| processing_hours | 0.0% |
| assigned_office | 0.0% |

No blocking issues found. Batch is clean for the quality gate.

## batch_2024_03.csv — BLOCKED

- Rows: 215
- Columns: `record_id, submitted_at, status, region, proc_hours_total, assigned_office`

**Null rate per column:**

| Column | Null % |
|---|---|
| record_id | 0.0% |
| submitted_at | 0.0% |
| status | 0.0% |
| region | 8.37% |
| proc_hours_total | 0.0% |
| assigned_office | 0.0% |

**Blocking issues:**

- Missing required column(s): processing_hours
- Unexpected column(s) not in declared schema: proc_hours_total (possible rename / schema drift from upstream)
- Null values in required field 'region': 18 row(s) (8.37% of batch)
- Duplicate primary keys on 'record_id': 30 row(s) across 15 duplicated key(s)

## batch_2024_04.csv — PASS

- Rows: 265
- Columns: `record_id, submitted_at, status, region, processing_hours, assigned_office`

**Null rate per column:**

| Column | Null % |
|---|---|
| record_id | 0.0% |
| submitted_at | 0.0% |
| status | 0.0% |
| region | 0.0% |
| processing_hours | 0.0% |
| assigned_office | 0.0% |

No blocking issues found. Batch is clean for the quality gate.
