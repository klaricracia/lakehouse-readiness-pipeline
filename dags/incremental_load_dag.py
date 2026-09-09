"""
Airflow DAG — Incremental Lakehouse Load (Production Orchestration Reference)
================================================================================
Author: Klarissa Artavia — Data & AI Strategy
GitHub: https://github.com/klaricracia
LinkedIn: https://www.linkedin.com/in/klariartavia/

This DAG documents how the audit -> quality gate -> incremental load
pipeline would be orchestrated in production, on a schedule, with Airflow
managing retries, alerting, and dependency ordering between the three
stages (and, on Databricks, running against a real Delta Lake table on
cloud storage instead of the local lake/ folder used by run_local.py).

This file is a REFERENCE / DOCUMENTATION artifact. It is not run as part
of this repo's demo — Airflow is deliberately not a dependency here because
spinning up a scheduler + metadata DB is heavy for a local portfolio demo.
To actually run and verify the pipeline in this repo, use `run_local.py`,
which executes the same three stages directly, in order, with no Airflow
installation required.

Task graph:
    readiness_audit_task >> quality_gate_task >> incremental_load_task
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


def _run_readiness_audit(**context):
    """Task 1: scan data/raw/, write output/readiness_report.md, push blocked batches to XCom."""
    from audit.readiness_audit import run_audit

    results = run_audit()
    blocked = [r["batch"] for r in results if r["verdict"] == "BLOCKED"]
    context["ti"].xcom_push(key="blocked_batches", value=blocked)
    return blocked


def _run_quality_gate(**context):
    """Task 2: validate every non-blocked batch with pandera; quarantine failures."""
    import pandas as pd
    from quality.checks import validate_batch, quarantine_batch

    blocked = context["ti"].xcom_pull(task_ids="readiness_audit", key="blocked_batches") or []
    raw_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
    batch_paths = sorted(glob.glob(os.path.join(raw_dir, "*.csv")))

    passed_batches = []
    for path in batch_paths:
        batch_name = os.path.basename(path)
        if batch_name in blocked:
            continue  # already blocked by the readiness audit — don't even attempt validation
        df = pd.read_csv(path)
        is_valid, errors = validate_batch(df, batch_name)
        if is_valid:
            passed_batches.append(batch_name)
        else:
            quarantine_batch(df, batch_name, errors)

    context["ti"].xcom_push(key="passed_batches", value=passed_batches)
    return passed_batches


def _run_incremental_load(**context):
    """Task 3: merge/upsert every batch that passed the gate into the Delta Lake table."""
    import pandas as pd
    from pipeline.incremental_load import load_batch

    passed = context["ti"].xcom_pull(task_ids="quality_gate", key="passed_batches") or []
    raw_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
    table_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lake", "service_requests")

    for batch_name in passed:
        df = pd.read_csv(os.path.join(raw_dir, batch_name))
        load_batch(df, table_path=table_path)


with DAG(
    dag_id="incremental_lakehouse_load",
    description="Audit -> quality gate -> incremental merge into the service_requests Delta Lake table",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",  # hourly — this is the "near real-time" cadence referenced in the README
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "incremental", "delta"],
) as dag:

    readiness_audit_task = PythonOperator(
        task_id="readiness_audit",
        python_callable=_run_readiness_audit,
    )

    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=_run_quality_gate,
    )

    incremental_load_task = PythonOperator(
        task_id="incremental_load",
        python_callable=_run_incremental_load,
    )

    readiness_audit_task >> quality_gate_task >> incremental_load_task
