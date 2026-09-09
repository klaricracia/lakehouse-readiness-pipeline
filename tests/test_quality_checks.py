"""
Tests — Quality Gate (quality/checks.py)
==========================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Covers: a clean batch passes validation; a batch with nulls in a required
field and duplicate primary keys is correctly blocked with specific reasons.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quality.checks import validate_batch, quarantine_batch


def make_clean_batch(n=5):
    return pd.DataFrame({
        "record_id": [f"SR-{i:06d}" for i in range(1, n + 1)],
        "submitted_at": ["2024-01-01T09:00:00"] * n,
        "status": ["submitted"] * n,
        "region": ["North Verdalia"] * n,
        "processing_hours": [float(i) for i in range(1, n + 1)],
        "assigned_office": ["Office-North-01"] * n,
    })


def test_clean_batch_passes():
    df = make_clean_batch()
    is_valid, errors = validate_batch(df, "clean_batch.csv")
    assert is_valid is True
    assert errors == []


def test_batch_with_nulls_is_blocked():
    df = make_clean_batch()
    df.loc[0, "region"] = None
    df.loc[2, "processing_hours"] = None
    is_valid, errors = validate_batch(df, "null_batch.csv")
    assert is_valid is False
    assert any("region" in e for e in errors)
    assert any("processing_hours" in e for e in errors)


def test_batch_with_duplicate_keys_is_blocked():
    df = make_clean_batch()
    df.loc[1, "record_id"] = df.loc[0, "record_id"]  # duplicate an existing key
    is_valid, errors = validate_batch(df, "dupe_batch.csv")
    assert is_valid is False
    assert any("Duplicate" in e for e in errors)


def test_batch_with_missing_required_column_is_blocked():
    df = make_clean_batch().rename(columns={"processing_hours": "proc_hours_total"})
    is_valid, errors = validate_batch(df, "drifted_batch.csv")
    assert is_valid is False
    assert any("processing_hours" in e for e in errors)


def test_batch_with_invalid_status_value_is_blocked():
    df = make_clean_batch()
    df.loc[0, "status"] = "not_a_real_status"
    is_valid, errors = validate_batch(df, "bad_status_batch.csv")
    assert is_valid is False


def test_quarantine_writes_csv_and_reason_file(tmp_path, monkeypatch):
    import quality.checks as checks_module

    monkeypatch.setattr(checks_module, "QUARANTINE_DIR", str(tmp_path))
    df = make_clean_batch()
    df.loc[0, "region"] = None
    csv_path, reason_path = quarantine_batch(df, "test_batch.csv", ["Null value in required field 'region'"])

    assert os.path.exists(csv_path)
    assert os.path.exists(reason_path)
    with open(reason_path) as f:
        content = f.read()
    assert "test_batch.csv" in content
    assert "QUARANTINED" in content
    assert "region" in content
