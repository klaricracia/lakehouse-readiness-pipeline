"""
Lakehouse Readiness Audit — Pre-Build Data Quality & Governance Scan
=====================================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

Pipeline:
  1. Discover      — find every raw batch file under data/raw/
  2. Schema check   — compare each batch's columns/dtypes against the
                       declared expected schema, flag any drift
  3. Null audit     — compute the null rate per column for each batch
  4. Duplicate audit— count duplicate primary keys (record_id) per batch
  5. Verdict        — PASS/BLOCKED per batch with specific, actionable
                       reasons, written to output/readiness_report.md

This is the gate that runs *before* any development work starts on top of
a batch. The intent mirrors a real pre-build data audit on a transformation
program: catch structural and governance problems early, in writing, before
a workstream is built on top of data that can't support it.

Usage:
    python audit/readiness_audit.py
"""

import os
import glob
import pandas as pd

# ── Declared expected schema (the contract every batch must satisfy) ───────
EXPECTED_SCHEMA = {
    "record_id":        "object",
    "submitted_at":     "object",
    "status":           "object",
    "region":           "object",
    "processing_hours": "float64",
    "assigned_office":  "object",
}
REQUIRED_COLUMNS = list(EXPECTED_SCHEMA.keys())
PRIMARY_KEY = "record_id"
REQUIRED_NON_NULL = ["record_id", "submitted_at", "status", "region", "processing_hours", "assigned_office"]

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
REPORT_PATH = os.path.join(OUTPUT_DIR, "readiness_report.md")


# ── Per-batch audit ──────────────────────────────────────────────────────────
def audit_batch(path):
    """Run the full readiness audit for a single raw batch file."""
    name = os.path.basename(path)
    issues = []

    # Read the file raw — do NOT force dtypes here, we want to see what the
    # source system actually sent us, drift and all.
    df = pd.read_csv(path)

    # 1. Schema drift check ---------------------------------------------------
    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_SCHEMA.keys())
    missing_cols = sorted(expected_cols - actual_cols)
    unexpected_cols = sorted(actual_cols - expected_cols)

    if missing_cols:
        issues.append(f"Missing required column(s): {', '.join(missing_cols)}")
    if unexpected_cols:
        issues.append(
            f"Unexpected column(s) not in declared schema: {', '.join(unexpected_cols)} "
            f"(possible rename / schema drift from upstream)"
        )

    type_drift = []
    for col in expected_cols & actual_cols:
        expected_dtype = EXPECTED_SCHEMA[col]
        actual_dtype = str(df[col].dtype)
        if expected_dtype == "float64" and actual_dtype not in ("float64", "int64"):
            type_drift.append(f"{col} (expected numeric, got {actual_dtype})")
    if type_drift:
        issues.append(f"Type drift: {', '.join(type_drift)}")

    # 2. Null-rate audit (only meaningful for columns that actually exist) ---
    null_rates = {}
    for col in df.columns:
        rate = df[col].isna().mean() * 100
        null_rates[col] = round(rate, 2)

    for col in REQUIRED_NON_NULL:
        if col in df.columns and null_rates.get(col, 0) > 0:
            null_count = int(df[col].isna().sum())
            issues.append(
                f"Null values in required field '{col}': {null_count} row(s) "
                f"({null_rates[col]}% of batch)"
            )

    # 3. Duplicate primary key audit ------------------------------------------
    dup_count = 0
    if PRIMARY_KEY in df.columns:
        dup_count = int(df[PRIMARY_KEY].duplicated(keep=False).sum())
        if dup_count > 0:
            unique_dupe_keys = df.loc[df[PRIMARY_KEY].duplicated(keep=False), PRIMARY_KEY].nunique()
            issues.append(
                f"Duplicate primary keys on '{PRIMARY_KEY}': {dup_count} row(s) "
                f"across {unique_dupe_keys} duplicated key(s)"
            )
    else:
        issues.append(f"Primary key column '{PRIMARY_KEY}' is missing — cannot dedupe or merge safely")

    verdict = "BLOCKED" if issues else "PASS"

    return {
        "batch": name,
        "rows": len(df),
        "columns": list(df.columns),
        "null_rates": null_rates,
        "duplicate_rows": dup_count,
        "verdict": verdict,
        "issues": issues,
    }


# ── Report rendering ─────────────────────────────────────────────────────────
def render_report(results):
    lines = []
    lines.append("# Lakehouse Readiness Audit Report")
    lines.append("")
    lines.append(
        "Pre-build data readiness audit over every raw batch in `data/raw/`, run "
        "before any batch is admitted to the quality gate or merged into the "
        "lakehouse table. Verdicts are computed against the declared schema in "
        "`audit/readiness_audit.py::EXPECTED_SCHEMA`."
    )
    lines.append("")

    blocked = [r for r in results if r["verdict"] == "BLOCKED"]
    passed = [r for r in results if r["verdict"] == "PASS"]

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Batches scanned: **{len(results)}**")
    lines.append(f"- Passed: **{len(passed)}**")
    lines.append(f"- Blocked: **{len(blocked)}**")
    lines.append("")

    lines.append("| Batch | Rows | Verdict | Blocking Issues |")
    lines.append("|---|---|---|---|")
    for r in results:
        issue_summary = f"{len(r['issues'])} issue(s)" if r["issues"] else "—"
        lines.append(f"| {r['batch']} | {r['rows']} | **{r['verdict']}** | {issue_summary} |")
    lines.append("")

    for r in results:
        lines.append(f"## {r['batch']} — {r['verdict']}")
        lines.append("")
        lines.append(f"- Rows: {r['rows']}")
        lines.append(f"- Columns: `{', '.join(r['columns'])}`")
        lines.append("")
        lines.append("**Null rate per column:**")
        lines.append("")
        lines.append("| Column | Null % |")
        lines.append("|---|---|")
        for col, rate in r["null_rates"].items():
            lines.append(f"| {col} | {rate}% |")
        lines.append("")
        if r["issues"]:
            lines.append("**Blocking issues:**")
            lines.append("")
            for issue in r["issues"]:
                lines.append(f"- {issue}")
        else:
            lines.append("No blocking issues found. Batch is clean for the quality gate.")
        lines.append("")

    return "\n".join(lines)


def run_audit():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batch_paths = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))

    print("\n" + "=" * 60)
    print("  LAKEHOUSE READINESS AUDIT")
    print("=" * 60)
    print(f"\nScanning {len(batch_paths)} batch(es) in data/raw/ ...\n")

    results = [audit_batch(p) for p in batch_paths]

    for r in results:
        marker = "PASS  " if r["verdict"] == "PASS" else "BLOCKED"
        print(f"  [{marker}] {r['batch']}  ({r['rows']} rows, {len(r['issues'])} issue(s))")
        for issue in r["issues"]:
            print(f"           - {issue}")

    report = render_report(results)
    with open(REPORT_PATH, "w") as f:
        f.write(report)

    blocked = [r["batch"] for r in results if r["verdict"] == "BLOCKED"]
    print(f"\n  Report written -> {os.path.relpath(REPORT_PATH)}")
    print("=" * 60 + "\n")

    return results


if __name__ == "__main__":
    run_audit()
