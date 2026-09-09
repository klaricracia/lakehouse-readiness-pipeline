# ADR 0002: Pre-Build Readiness Audit as a Hard Gate, Not a Warning

## Status
Accepted

## Context
Before any pipeline development started on this program, the incoming
batches were audited for structural and governance issues — schema drift
against a declared contract, null rates in required fields, and duplicate
primary keys — using `audit/readiness_audit.py`.

That audit surfaced real, blocking problems in one of the four sample
batches (`batch_2024_03.csv`): a renamed/missing required column
(`processing_hours` arrived as `proc_hours_total`), 18 rows with a null
`region`, and 15 duplicate `record_id` values. In the broader program this
audit is modeled on, the same category of finding — a source system
silently renaming a column, or a key that isn't actually unique — is
exactly the kind of blocker that has previously eliminated entire
downstream workstreams once discovered mid-build, after weeks of transform
and report logic had already been written against a schema that didn't
hold.

The open question was *what to do with a batch that fails the audit*:

1. **Warning only** — log the issue, but let the batch continue through
   the pipeline. Someone reviews the log later.
2. **Hard gate** — a batch that fails the readiness audit (or the
   downstream quality gate) is quarantined and never reaches the
   lakehouse table, automatically, with no manual step required to stop
   it.

## Decision
Failing the readiness audit / quality gate is a hard stop, not a warning.
A batch that is blocked or invalid is written to `output/quarantine/` with
a specific, itemized reason and is excluded from the merge step entirely
(`run_local.py` and `dags/incremental_load_dag.py` both skip quarantined
batches — they never reach `pipeline/incremental_load.py`).

## Consequences

**Positive:**
- A duplicate primary key or a null in a required field can never silently
  reach the lakehouse table and corrupt a downstream merge or report — the
  failure mode is "batch doesn't load, with a written reason," not "table
  has bad data that surfaces three reports downstream."
- Because the report and reasons are written to disk automatically
  (`output/readiness_report.md`, `output/quarantine/*_REASON.txt`), the
  audit produces a paper trail on every run, not just when someone
  remembers to check a log. That's what makes it usable as an actual
  go/no-go decision point before a workstream is built, rather than
  something that's only informative in hindsight.
- Forces the real question — "is this batch's schema and key integrity
  actually reliable" — to be answered before development effort is spent
  building on top of it, rather than discovered when a report breaks in
  production.

**Negative / tradeoffs:**
- A warning-only approach would have let all four sample batches flow
  through, including the corrupted one — which is a strictly worse outcome
  for a lakehouse table's integrity, but a gate does mean a legitimately
  urgent batch with a minor issue is blocked along with a genuinely broken
  one. There is no "quarantine with a manual override" path in this repo;
  in a production version, a reviewed manual re-admit step (fix the
  batch upstream, resubmit) would sit next to this gate rather than
  bypassing it.
- The gate is only as good as the declared contract it checks against
  (`EXPECTED_SCHEMA` in `audit/readiness_audit.py`, the `pandera` schema in
  `quality/checks.py`). A quality problem the contract doesn't encode
  (e.g. a `processing_hours` value that's numerically valid but
  operationally implausible) will still pass. The contract is expected to
  grow as new failure modes are discovered — it is not a one-time
  definition.
