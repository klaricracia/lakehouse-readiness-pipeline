# ADR 0001: Incremental Merge Over Full Reload

## Status
Accepted

## Context
The lakehouse table (`service_requests`) receives new and updated case
records in batches from an upstream case-processing system. The original
(pre-project) reporting process reloaded the entire dataset from source on
every run — truncate, re-read every historical file, rebuild the table —
before regenerating downstream reports. On the full historical volume this
took roughly 4 hours end to end, and reports were only as fresh as the last
overnight run.

Two options were on the table for how this pipeline ingests new batches:

1. **Full reload** — on every run, re-read every batch ever received and
   rebuild the table from scratch. Simple, but the cost of a run grows with
   total history, not with how much actually changed, and correctness
   depends on every historical file staying available and unchanged.
2. **Incremental merge/upsert** — on every run, take only the new batch and
   merge it into the existing table, keyed on `record_id`: new keys are
   inserted, existing keys are updated in place, and everything else in the
   table is left untouched.

## Decision
Use incremental merge/upsert, keyed on `record_id`, implemented with
`deltalake`'s `DeltaTable.merge()` (`when_matched_update_all` /
`when_not_matched_insert_all`). See `pipeline/incremental_load.py`.

Each batch is merged directly into the table rather than appended blindly.
This is what makes the pipeline idempotent: re-running the same batch does
not duplicate rows, because matching keys are updated, not re-inserted.
Idempotency is what makes it safe to run on a schedule (or re-run after a
failure) without manual cleanup — a property a full reload doesn't need,
but that a scheduled incremental pipeline cannot skip.

## Consequences

**Positive:**
- Run time scales with the size of the incoming batch, not with total
  table history — this is the mechanism behind the "4 hours to near
  real-time" latency improvement referenced in the project README. A batch
  of a few hundred records merges in well under a second locally; a full
  reload of the whole table does not.
- Safe to schedule frequently (hourly, per the reference Airflow DAG)
  because re-running a batch is a no-op on row count, not a duplication
  risk.
- Late-arriving corrections (a case whose status changes after the fact)
  are handled naturally — the merge updates the existing row instead of
  requiring a separate "corrections" process.

**Negative / tradeoffs:**
- Merge logic is more complex than "append everything" or "truncate and
  reload" — it depends on a reliable primary key (`record_id`) being
  present and genuinely unique per batch, which is exactly the kind of
  thing the readiness audit (ADR 0002) checks *before* a batch reaches the
  merge step.
- Deletes are not handled by this pipeline. If a record needs to be
  removed from the source of truth, upstream would need to emit an
  explicit tombstone/soft-delete record (e.g. `status = "deleted"`) rather
  than simply omitting it from a batch — omission and deletion are
  indistinguishable to a keyed merge. Not needed for the case-processing
  data modeled here, but worth flagging for any future dataset where hard
  deletes matter.
- Debugging "what did batch N actually change" requires diffing against
  the prior table state, since the table itself only reflects the latest
  merged value per key, not a full change history. Delta Lake's built-in
  transaction log (time travel) mitigates this if it's ever needed.
