"""Manually orchestrate the approved synthetic M1 ingestion/reconciliation slice.

Business rules remain in retail_ingestion and the existing dbt-backed
retail_reconciliation module. Only the committed snapshot cutoff crosses XCom.
"""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator


def ingest_sources_task():
    """Use the existing per-source atomic ingestion and replay checks."""
    try:
        import json

        from retail_ingestion import connect_from_env, ingest_batch, load_manifest

        batches = load_manifest("/workspace/fixtures/reconciliation/manifest.json")
        connection = connect_from_env()
        try:
            for batch in batches:
                result = ingest_batch(connection, batch)
                print(json.dumps({
                    "synthetic": True,
                    "source_name": result["source_name"],
                    "batch_id": result["batch_id"],
                    "batch_sequence": result["batch_sequence"],
                    "row_count": result["row_count"],
                    "action": result["action"],
                }, sort_keys=True))
        finally:
            connection.close()
    except Exception as error:
        # Driver errors can contain connection details; propagate only their type.
        raise RuntimeError(type(error).__name__) from None


def capture_snapshot_task():
    """Return one non-sensitive, validated committed sequence to XCom."""
    try:
        from retail_ingestion import connect_from_env

        connection = connect_from_env()
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT COALESCE(max(batch_sequence), 0) "
                        "FROM bronze.input_batches WHERE status = 'COMMITTED'"
                    )
                    cutoff = cursor.fetchone()[0]
            if type(cutoff) is not int or not 0 <= cutoff <= 9223372036854775807:
                raise ValueError("Invalid nonnegative BIGINT snapshot cutoff")
            print("COMMITTED snapshot_cutoff=" + str(cutoff))
            return cutoff
        finally:
            connection.close()
    except Exception as error:
        raise RuntimeError(type(error).__name__) from None


def reconcile_snapshot_task(snapshot_cutoff, evaluation_as_of, rules_version,
                            fail_reconcile_once, failure_option_is_boolean,
                            try_number):
    """Invoke the existing evaluator; optional local failure occurs before writes."""
    if failure_option_is_boolean is not True or type(fail_reconcile_once) is not bool:
        raise RuntimeError("fail_reconcile_once must be a JSON boolean")
    if type(try_number) is not int or try_number < 1:
        raise RuntimeError("Invalid Airflow task attempt number")
    # Manual synthetic diagnosis exercise only; clearing this task preserves its
    # upstream cutoff and permits attempt 2 without changing source/business data.
    if fail_reconcile_once and try_number == 1:
        raise RuntimeError("Controlled synthetic failure before reconciliation")
    try:
        import json
        import os
        import sys
        from pathlib import Path

        from retail_reconciliation import Context, evaluate

        # Existing compile_model launches bare `dbt`; isolate its executable too.
        os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")
        context = Context(snapshot_cutoff, evaluation_as_of, rules_version)
        result = evaluate(context)
        print(json.dumps({
            key: result[key] for key in (
                "run_id", "evaluation_key", "snapshot_cutoff",
                "evaluation_as_of", "rules_version", "action",
            )
        }, sort_keys=True))
    except Exception as error:
        raise RuntimeError(type(error).__name__) from None


with DAG(
    dag_id="retail_m1_thin_slice",
    description="Manual synthetic Orders/Payments/Refunds M1 demonstration",
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={"retries": 0},
    tags=["retail", "synthetic", "m1"],
) as dag:
    operator_options = {
        "python": "/opt/airflow/retail-venv/bin/python",
        "serializer": "pickle",
        "expect_airflow": False,
        "inherit_env": True,
        "show_return_value_in_logs": False,
    }
    ingest_sources = ExternalPythonOperator(
        task_id="ingest_sources",
        python_callable=ingest_sources_task,
        do_xcom_push=False,
        **operator_options,
    )
    capture_snapshot = ExternalPythonOperator(
        task_id="capture_snapshot",
        python_callable=capture_snapshot_task,
        **operator_options,
    )
    reconcile_snapshot = ExternalPythonOperator(
        task_id="reconcile_snapshot",
        python_callable=reconcile_snapshot_task,
        op_kwargs={
            "snapshot_cutoff": "{{ ti.xcom_pull(task_ids='capture_snapshot') }}",
            "evaluation_as_of": "2026-02-03T00:00:00Z",
            "rules_version": "m1-thin-v1",
            "fail_reconcile_once": "{{ (dag_run.conf or {}).get('fail_reconcile_once', false) }}",
            # Check before native rendering can coerce strings such as "True".
            "failure_option_is_boolean": "{{ (dag_run.conf or {}).get('fail_reconcile_once', false) is boolean }}",
            "try_number": "{{ ti.try_number }}",
        },
        do_xcom_push=False,
        **operator_options,
    )

    ingest_sources >> capture_snapshot >> reconcile_snapshot
