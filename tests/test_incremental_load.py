"""
Tests — Incremental Loader (pipeline/incremental_load.py)
=============================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Covers: loading a batch into a fresh table; loading the same batch twice is
idempotent (no duplicate rows); loading a batch with an updated record
correctly upserts it (in place, not as a new row).
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.incremental_load import load_batch, read_table


def make_batch(ids, statuses=None):
    n = len(ids)
    statuses = statuses or ["submitted"] * n
    return pd.DataFrame({
        "record_id": ids,
        "submitted_at": ["2024-01-01T09:00:00"] * n,
        "status": statuses,
        "region": ["North Verdalia"] * n,
        "processing_hours": [float(i) for i in range(n)],
        "assigned_office": ["Office-North-01"] * n,
    })


def test_initial_load_creates_table(tmp_path):
    table_path = str(tmp_path / "service_requests")
    df = make_batch(["SR-000001", "SR-000002", "SR-000003"])
    result = load_batch(df, table_path=table_path)

    assert result["mode"] == "initial_write"
    assert result["rows_after"] == 3

    table = read_table(table_path)
    assert len(table) == 3
    assert set(table["record_id"]) == {"SR-000001", "SR-000002", "SR-000003"}


def test_loading_same_batch_twice_is_idempotent(tmp_path):
    table_path = str(tmp_path / "service_requests")
    df = make_batch(["SR-000001", "SR-000002", "SR-000003"])

    load_batch(df, table_path=table_path)
    result_second_run = load_batch(df, table_path=table_path)

    table = read_table(table_path)
    assert len(table) == 3  # no duplicates
    assert result_second_run["rows_after"] == 3
    assert result_second_run["rows_inserted"] == 0
    assert result_second_run["rows_updated"] == 3


def test_loading_new_batch_appends_new_records(tmp_path):
    table_path = str(tmp_path / "service_requests")
    load_batch(make_batch(["SR-000001", "SR-000002"]), table_path=table_path)
    load_batch(make_batch(["SR-000003", "SR-000004"]), table_path=table_path)

    table = read_table(table_path)
    assert len(table) == 4
    assert set(table["record_id"]) == {"SR-000001", "SR-000002", "SR-000003", "SR-000004"}


def test_loading_batch_with_updated_record_upserts_in_place(tmp_path):
    table_path = str(tmp_path / "service_requests")
    load_batch(make_batch(["SR-000001", "SR-000002"], statuses=["submitted", "submitted"]), table_path=table_path)

    update_df = make_batch(["SR-000001"], statuses=["resolved"])
    result = load_batch(update_df, table_path=table_path)

    table = read_table(table_path)
    assert len(table) == 2  # still 2 rows — the update did not add a new row
    assert result["rows_updated"] == 1
    assert result["rows_inserted"] == 0

    updated_row = table[table["record_id"] == "SR-000001"].iloc[0]
    assert updated_row["status"] == "resolved"
