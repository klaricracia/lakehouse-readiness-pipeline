# Lakehouse Readiness Pipeline

> **CASE STUDY** · Data Engineering · Python · Delta Lake · Data Governance · Incremental Processing

---

## The Problem

On a national-scale case-processing transformation program (modeled here on a
fictional government "service requests" system — entirely synthetic data,
no real client or agency data), reporting ran on a nightly full reload: every
historical batch re-read from scratch, the whole table rebuilt, before any
downstream report could refresh. That took hours, and it meant reports were
never fresher than the last overnight run — and it assumed every batch that
ever arrived was trustworthy enough to build on.

It usually wasn't. Before any pipeline development starts on a program like
this, the raw batches need a structured readiness audit — not a spot check,
a real scan for schema drift, null rates in required fields, and duplicate
keys — because a workstream built on top of a batch with a broken primary
key or a silently renamed column doesn't fail loudly in development. It
fails quietly in production, usually inside someone else's report.

This project is a small, honest, runnable version of the fix: a pre-build
readiness audit that produces a written pass/fail verdict per batch, an
automated quality gate that blocks bad batches from ever reaching the
table, and an idempotent incremental merge that replaces the full reload.

**Being upfront about scope:** this is a local, dependency-light demo. It
uses a real Delta Lake table format via the `deltalake` Python package —
not a Databricks cluster, not cloud storage, not a live Airflow scheduler.
The Airflow DAG in `dags/` is a production orchestration *reference*, not
something this repo runs. `run_local.py` is what actually runs, end to
end, with no orchestration platform required.

---

## Architecture

```
data/raw/*.csv
      │
      ▼
┌─────────────────────┐
│ 1. Readiness Audit   │  audit/readiness_audit.py
│  schema drift        │  → output/readiness_report.md
│  null rates          │    (pass/fail verdict per batch,
│  duplicate keys       │     specific blocking issues)
└─────────┬────────────┘
          │  clean batches only
          ▼
┌─────────────────────┐
│ 2. Quality Gate       │  quality/checks.py (pandera)
│  required columns     │  fail → output/quarantine/*.csv
│  correct types         │         + *_REASON.txt
│  key uniqueness        │  pass → continue
│  no nulls in required  │
└─────────┬────────────┘
          │  validated batches only
          ▼
┌─────────────────────┐
│ 3. Incremental Load   │  pipeline/incremental_load.py
│  MERGE keyed on        │  → lake/service_requests
│  record_id             │    (local Delta Lake table)
│  idempotent upsert      │
└──────────────────────┘
```

`run_local.py` runs all three stages, in order, over every batch in
`data/raw/`, with no external services required. `dags/incremental_load_dag.py`
documents the same three stages wired as an Airflow DAG for a production
deployment, where this would run hourly against a real cluster instead of a
local folder.

---

## Stage Breakdown

### 1. Readiness Audit (`audit/readiness_audit.py`)
Scans every batch in `data/raw/` against a declared expected schema
(`EXPECTED_SCHEMA`) and reports, per batch:
- **Schema drift** — missing required columns, unexpected/renamed columns,
  type drift
- **Null rate per column**
- **Duplicate primary keys** on `record_id`
- A **PASS/BLOCKED verdict** with the specific, itemized reasons — written
  to `output/readiness_report.md`

This runs before any batch is even offered to the quality gate. It's the
"should we build on this" question, answered in writing, on every run.

### 2. Quality Gate (`quality/checks.py`)
A hard validation gate that runs immediately before merge. Enforces:
required columns present, correct types, `record_id` uniqueness, no nulls
in required fields, and a closed set of valid `status` values. Batches that
fail are **quarantined** to `output/quarantine/` (the batch CSV plus a
`_REASON.txt` with the specific failures) and are never merged.

**Library used: [`pandera`](https://pandera.readthedocs.io/).** It
installed cleanly and quickly in this project's venv and gives lazy
(collect-every-failure-in-one-pass) validation with structured failure
output out of the box, which is why it was picked over writing a
hand-rolled checker for this demo.

### 3. Incremental Load (`pipeline/incremental_load.py`)
Merges each validated batch into a **local Delta Lake table** at
`lake/service_requests`, keyed on `record_id`, using the `deltalake`
Python package's native `DeltaTable.merge()` (`when_matched_update_all` /
`when_not_matched_insert_all`). Matching keys are updated in place; new
keys are inserted. Nothing is ever blindly appended — see
[ADR 0001](docs/adr/0001-incremental-vs-full-reload.md) for why.

**Library used: `deltalake` (real Delta Lake, not a Parquet fallback).**
It installed and ran cleanly for this demo, including native `MERGE`
semantics — no fallback to hand-rolled Parquet upsert logic was needed.

### 4. Orchestration reference (`dags/incremental_load_dag.py`)
A syntactically valid Airflow DAG wiring the same three stages
(`readiness_audit >> quality_gate >> incremental_load` as `PythonOperator`
tasks) on an hourly schedule. This documents how the pipeline would be
orchestrated in production; it is **not** run as part of this repo's demo
— see "How to Run" below.

---

## Results (Actual Local Run)

Four synthetic batches, run through `run_local.py` end to end:

| Batch | Rows | Audit Verdict | Quality Gate | Table Change |
|---|---|---|---|---|
| `batch_2024_01.csv` | 300 | PASS | merged | +300 inserted |
| `batch_2024_02.csv` | 315 | PASS | merged | +300 inserted, 15 updated |
| `batch_2024_03.csv` | 215 | **BLOCKED** | **quarantined** | not merged |
| `batch_2024_04.csv` | 265 | PASS | merged | +250 inserted, 15 updated |

**Final lakehouse table: 850 rows, 850 unique `record_id` values.**
(300 + 300 + 250 new records across the three clean batches; the 30 update
rows in batches 2 and 4 correctly upserted existing records instead of
adding new ones; batch 3's 215 rows never reached the table at all.)

**What the readiness audit caught in `batch_2024_03.csv`, correctly and
automatically, before any merge was attempted:**
- Missing required column `processing_hours` (renamed to `proc_hours_total`
  upstream — genuine schema drift, not simulated after the fact)
- 18 rows (8.37%) with a null `region`, a required field
- 30 rows sharing 15 duplicate `record_id` values

All three issues are real, present in the seeded CSV itself — this is the
kind of finding that, caught after the fact instead of before development,
is exactly what eliminates a workstream mid-build: a report built on the
assumption that `record_id` is unique, or that `processing_hours` exists
and is numeric, breaks the moment it hits this batch.

**Idempotency, verified directly:** running `run_local.py` a second time
over the same four batches leaves the table at exactly 850 rows — every
merged batch reports 0 inserted (300, 315, and 265 rows respectively all
matched and were upserted in place, none duplicated). Batch 3 is
quarantined identically on every run.

**Tests:** `python -m pytest tests/` — **10/10 passing**, covering the
quality gate (clean batch passes; nulls, duplicate keys, schema drift, and
invalid status values are each independently blocked with a specific
reason; quarantine writes both the CSV and the reason file) and the
incremental loader (initial load, idempotent re-load, new-record insert,
and in-place upsert of an updated record).

---

## Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11+ |
| Data manipulation | pandas, numpy |
| Quality gate | pandera |
| Lakehouse table | deltalake (Delta Lake, local) |
| Table format | pyarrow |
| Orchestration (reference only) | Airflow (DAG definition, not executed here) |
| Testing | pytest |

---

## How to Run

```bash
# Clone the repo
git clone https://github.com/klaricracia/lakehouse-readiness-pipeline.git
cd lakehouse-readiness-pipeline

# Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline: audit -> quality gate -> incremental load
python run_local.py

# Inspect the outputs
cat output/readiness_report.md            # audit findings per batch
cat output/quarantine/batch_2024_03_REASON.txt   # why batch 3 was blocked
python -c "from pipeline.incremental_load import read_table; print(read_table().shape)"

# Run again to see idempotent behaviour (row count won't change)
python run_local.py

# Run the tests
python -m pytest tests/ -v
```

`dags/incremental_load_dag.py` is not run in this repo — it requires an
Airflow environment (scheduler + metadata DB) that's out of scope for a
local demo. It's included as a production orchestration reference; read it
alongside `run_local.py` to see the same three stages wired both ways.

---

## How to Extend This

- **Point it at a real cluster:** swap the local `lake/service_requests`
  path for a Databricks-managed Delta table (Unity Catalog path or DBFS/S3
  URI) — `write_deltalake`/`DeltaTable.merge()` calls don't change.
- **Run it on a schedule for real:** deploy `dags/incremental_load_dag.py`
  to an actual Airflow instance; the task bodies already import directly
  from `audit/`, `quality/`, and `pipeline/`, so no logic needs to move.
- **Add more governance checks:** `audit/readiness_audit.py`'s
  `EXPECTED_SCHEMA` and `quality/checks.py`'s `BATCH_SCHEMA` are both small,
  declarative contracts — add a new column or constraint in one place and
  both the audit and the gate pick it up.
- **Add a manual re-admit path:** right now a quarantined batch stays
  quarantined; a reviewed "fix upstream, resubmit" flow would sit next to
  the automated gate rather than bypassing it (see
  [ADR 0002](docs/adr/0002-pre-build-readiness-audit-as-a-gate.md)).
- **Track schema contract changes over time:** version `EXPECTED_SCHEMA`
  itself and diff it release over release, so drift in the *contract* is
  as visible as drift in the *data*.

---

*Built by [Klarissa Artavia](https://www.linkedin.com/in/klariartavia/) · Data & AI Strategy*
*GitHub: [github.com/klaricracia](https://github.com/klaricracia)*
