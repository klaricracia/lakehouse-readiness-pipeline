"""
Incremental Lakehouse Loader — Idempotent Merge/Upsert into Delta Lake
=========================================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Pipeline:
  1. Load target      — open (or create) the Delta Lake table at
                         lake/service_requests
  2. Merge/upsert      — MERGE the incoming batch into the table keyed on
                          record_id: matched rows are updated in place,
                          unmatched rows are inserted — nothing is ever
                          appended blindly
  3. Verify            — report row counts before/after so incremental
                          (not full-reload) behaviour is visible on every run

This is the "reduced reporting latency from 4 hours to near real-time"
piece: instead of a nightly full reload of the whole table, each new batch
is merged in directly, in seconds, keyed on its primary key. Re-running the
same batch twice is a no-op on row count — that's the idempotency guarantee
a real incremental pipeline has to have before it can be trusted to run on
a schedule. See docs/adr/0001-incremental-vs-full-reload.md.

Table format: a real local Delta Lake table, written with the `deltalake`
Python package (`write_deltalake` + `DeltaTable.merge`) — no Databricks
cluster involved, this is the dependency-light local stand-in described in
the README, but the Delta table format and merge semantics are real.

Usage (as a library):
    from pipeline.incremental_load import load_batch
    result = load_batch(df, table_path="lake/service_requests")
"""

import os
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

PRIMARY_KEY = "record_id"


def _table_exists(table_path: str) -> bool:
    try:
        DeltaTable(table_path)
        return True
    except TableNotFoundError:
        return False
    except Exception:
        return False


def load_batch(df: pd.DataFrame, table_path: str = "lake/service_requests", primary_key: str = PRIMARY_KEY):
    """
    Idempotently merge/upsert `df` into the Delta Lake table at `table_path`,
    keyed on `primary_key`. Rows whose key already exists are updated in
    place; new keys are inserted. Running the same batch twice does not
    duplicate rows.

    Returns a dict with before/after row counts and how many rows were new
    vs. updated.
    """
    os.makedirs(os.path.dirname(table_path) or ".", exist_ok=True)

    if not _table_exists(table_path):
        write_deltalake(table_path, df, mode="overwrite")
        return {
            "rows_before": 0,
            "rows_in_batch": len(df),
            "rows_after": len(df),
            "rows_inserted": len(df),
            "rows_updated": 0,
            "mode": "initial_write",
        }

    dt = DeltaTable(table_path)
    rows_before = len(dt.to_pandas())
    existing_ids = set(dt.to_pandas()[primary_key])
    incoming_ids = set(df[primary_key])
    rows_updated = len(existing_ids & incoming_ids)
    rows_inserted = len(incoming_ids - existing_ids)

    (
        dt.merge(
            source=df,
            predicate=f"target.{primary_key} = source.{primary_key}",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    rows_after = len(DeltaTable(table_path).to_pandas())

    return {
        "rows_before": rows_before,
        "rows_in_batch": len(df),
        "rows_after": rows_after,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "mode": "merge",
    }


def read_table(table_path: str = "lake/service_requests") -> pd.DataFrame:
    """Read the current state of the lakehouse table as a DataFrame."""
    if not _table_exists(table_path):
        return pd.DataFrame()
    return DeltaTable(table_path).to_pandas()
