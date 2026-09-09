"""
Quality Gate — Pre-Merge Validation for the Lakehouse Pipeline
=================================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Pipeline:
  1. Schema contract — declared with pandera: required columns, correct
                        types, non-null required fields, unique primary key
  2. Validate         — every batch is validated with `lazy=True` so ALL
                         failures are collected in one pass, not just the
                         first one
  3. Route            — batches that pass are handed to the incremental
                         loader; batches that fail are quarantined to
                         output/quarantine/ with a written reason and are
                         NOT merged into the lakehouse table

This is the hard gate between "data arrived" and "data is in the lakehouse
table." Nothing reaches `pipeline/incremental_load.py` without passing here
first — see docs/adr/0002-pre-build-readiness-audit-as-a-gate.md for why
this is a gate and not a warning.

Library choice: pandera. It installed cleanly and quickly in this project's
venv and gives lazy (collect-all-failures) validation with structured
failure output for free, which is why it was picked over a hand-rolled
checker for this demo.

Usage (as a library):
    from quality.checks import validate_batch
    is_valid, errors = validate_batch(df, batch_name="batch_2024_03.csv")
"""

import os
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check

PRIMARY_KEY = "record_id"
REQUIRED_COLUMNS = ["record_id", "submitted_at", "status", "region", "processing_hours", "assigned_office"]

QUARANTINE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output", "quarantine"
)

# ── Schema contract enforced before merge ───────────────────────────────────
BATCH_SCHEMA = DataFrameSchema(
    {
        "record_id": Column(str, nullable=False, unique=True, coerce=True),
        "submitted_at": Column(str, nullable=False, coerce=True),
        "status": Column(
            str,
            nullable=False,
            coerce=True,
            checks=Check.isin(["submitted", "in_review", "resolved", "closed", "rejected"]),
        ),
        "region": Column(str, nullable=False, coerce=True),
        "processing_hours": Column(float, nullable=False, coerce=True, checks=Check.ge(0)),
        "assigned_office": Column(str, nullable=False, coerce=True),
    },
    strict=False,  # extra columns are tolerated by the gate; missing/invalid required ones are not
    coerce=False,
)


def validate_batch(df: pd.DataFrame, batch_name: str = "batch"):
    """
    Validate a batch DataFrame against the schema contract.

    Returns:
        (is_valid: bool, errors: list[str]) — errors is a list of
        human-readable, specific failure reasons (empty if valid).
    """
    try:
        BATCH_SCHEMA.validate(df, lazy=True)
        return True, []
    except pa.errors.SchemaErrors as exc:
        errors = []
        cases = exc.failure_cases
        for _, row in cases.iterrows():
            check = row.get("check")
            column = row.get("column")
            failure_case = row.get("failure_case")
            if check == "column_in_dataframe":
                errors.append(f"Required column missing: '{failure_case}'")
            elif check == "not_nullable":
                errors.append(f"Null value in required field '{column}'")
            elif check == "field_uniqueness":
                errors.append(f"Duplicate value in primary key '{column}': '{failure_case}'")
            elif column:
                errors.append(f"Validation failed on column '{column}' (check: {check}, value: {failure_case})")
            else:
                errors.append(f"Validation failed (check: {check}, value: {failure_case})")
        # De-duplicate while preserving order (many duplicate-key rows produce many identical messages)
        seen = set()
        deduped = []
        for e in errors:
            if e not in seen:
                seen.add(e)
                deduped.append(e)
        return False, deduped


def quarantine_batch(df: pd.DataFrame, batch_name: str, errors: list):
    """Write a failing batch and its reasons to output/quarantine/ instead of merging it."""
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    stem = os.path.splitext(batch_name)[0]

    csv_path = os.path.join(QUARANTINE_DIR, f"{stem}.csv")
    df.to_csv(csv_path, index=False)

    reason_path = os.path.join(QUARANTINE_DIR, f"{stem}_REASON.txt")
    with open(reason_path, "w") as f:
        f.write(f"Batch: {batch_name}\n")
        f.write(f"Status: QUARANTINED — not merged into the lakehouse table\n")
        f.write(f"Rows: {len(df)}\n\n")
        f.write("Reasons:\n")
        for e in errors:
            f.write(f"  - {e}\n")

    return csv_path, reason_path
