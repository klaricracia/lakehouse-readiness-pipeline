"""
Local Pipeline Runner — Audit -> Quality Gate -> Incremental Load
=====================================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Pipeline:
  1. Readiness audit  — scan every batch in data/raw/, write
                         output/readiness_report.md
  2. Quality gate      — validate each batch with pandera; clean batches
                          proceed, failing batches are quarantined to
                          output/quarantine/ and skipped
  3. Incremental load  — merge/upsert each surviving batch into the local
                          Delta Lake table at lake/service_requests, in
                          batch order, and print row counts at every step

This is the dependency-light way to run and verify the whole pipeline
end-to-end without Airflow installed. `dags/incremental_load_dag.py` shows
how the same three steps would be wired as an Airflow DAG in production —
it is a reference/documentation artifact, not something this script needs.

Usage:
    python run_local.py
"""

import glob
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from audit.readiness_audit import run_audit
from quality.checks import validate_batch, quarantine_batch
from pipeline.incremental_load import load_batch, read_table

RAW_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lake", "service_requests")


def main():
    print("\n" + "#" * 60)
    print("#  LAKEHOUSE READINESS PIPELINE — LOCAL RUN")
    print("#" * 60)

    # ── Stage 1: Readiness audit ────────────────────────────────────────────
    audit_results = run_audit()
    blocked_by_audit = {r["batch"] for r in audit_results if r["verdict"] == "BLOCKED"}

    # ── Stage 2 & 3: Quality gate -> Incremental load, per batch, in order ──
    print("\n" + "=" * 60)
    print("  QUALITY GATE + INCREMENTAL LOAD")
    print("=" * 60 + "\n")

    batch_paths = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    summary = []

    for path in batch_paths:
        batch_name = os.path.basename(path)
        df = pd.read_csv(path)

        is_valid, errors = validate_batch(df, batch_name)

        if not is_valid:
            csv_path, reason_path = quarantine_batch(df, batch_name, errors)
            print(f"  [QUARANTINED] {batch_name} — {len(errors)} issue(s), NOT merged")
            for e in errors:
                print(f"                - {e}")
            print(f"                -> {os.path.relpath(csv_path)}")
            summary.append({"batch": batch_name, "status": "QUARANTINED", "rows_in_batch": len(df),
                             "rows_inserted": 0, "rows_updated": 0})
            continue

        result = load_batch(df, table_path=TABLE_PATH)
        print(f"  [MERGED]       {batch_name} — {result['rows_inserted']} inserted, "
              f"{result['rows_updated']} updated -> table now {result['rows_after']} rows")
        summary.append({"batch": batch_name, "status": "MERGED", "rows_in_batch": len(df),
                         "rows_inserted": result["rows_inserted"], "rows_updated": result["rows_updated"]})

    # ── Final state ──────────────────────────────────────────────────────────
    final_table = read_table(TABLE_PATH)
    print("\n" + "=" * 60)
    print("  RUN SUMMARY")
    print("=" * 60)
    for row in summary:
        print(f"  {row['batch']:<24} {row['status']:<12} "
              f"in={row['rows_in_batch']:<5} inserted={row['rows_inserted']:<5} updated={row['rows_updated']}")
    print(f"\n  Batches blocked by audit:  {sorted(blocked_by_audit)}")
    print(f"  Final lakehouse row count: {len(final_table)}  (unique record_id: {final_table['record_id'].nunique() if len(final_table) else 0})")
    print(f"  Table path: {os.path.relpath(TABLE_PATH)}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
