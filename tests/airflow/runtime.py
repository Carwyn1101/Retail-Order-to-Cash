"""Native scheduler/API/PostgreSQL evidence for the synthetic M1-T04 DAG.

Run inside the existing Airflow webserver, after the sequential T01/T02/T03
regressions. This client triggers real manual runs; it never calls ``dags test``
or changes Retail source/canonical records. Only its own failed task is cleared.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import quote
import uuid

import psycopg2
from psycopg2 import sql
import requests


DAG_ID = "retail_m1_thin_slice"
TASK_IDS = ("ingest_sources", "capture_snapshot", "reconcile_snapshot")
AS_OF = "2026-02-03T00:00:00.000000Z"
RULES = "m1-thin-v1"
EXPECTED = {
    "syn-t03-f01": (10000, 10000, 0, 10000, "MATCHED", []),
    "syn-t03-f03": (4500, 0, 0, 0, "EXCEPTION", ["MISSING_PAYMENT"]),
    "syn-t03-f08": (8000, 8000, 2000, 6000, "MATCHED", []),
}
TABLES = (
    ("bronze", "input_batches"), ("bronze", "orders_source"),
    ("bronze", "payments_source"), ("bronze", "refunds_source"),
    ("core", "reconciliation_results"), ("core", "reconciliation_findings"),
    ("core", "dq_findings"),
)
SECRET_NAMES = (
    "POSTGRES_PASSWORD", "RETAIL_DB_PASSWORD", "AIRFLOW_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_WEBSERVER_SECRET_KEY",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__WEBSERVER__SECRET_KEY",
)


class CheckFailed(AssertionError):
    """A fixed, nonsensitive acceptance assertion label."""


def require(condition, label):
    if not condition:
        raise CheckFailed(label)


def clean(text):
    for name in SECRET_NAMES:
        value = os.environ.get(name)
        if value:
            text = text.replace(value, "[REDACTED]")
    return text


def emit(event, **fields):
    print(clean(json.dumps({"event": event, **fields}, sort_keys=True)), flush=True)


def retail_connection():
    return psycopg2.connect(
        host=os.environ["PGHOST"], port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
        password=os.environ["RETAIL_DB_PASSWORD"], connect_timeout=10,
    )


def query(statement, params=()):
    with retail_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, params)
            return cursor.fetchall()


def snapshot(label):
    """Complete persisted business rows, ordered independently of physical order."""
    state = {}
    with retail_connection() as connection:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with connection.cursor() as cursor:
            for schema, table in TABLES:
                cursor.execute(sql.SQL(
                    "SELECT coalesce(jsonb_agg(row_value ORDER BY row_value::text), '[]'::jsonb) "
                    "FROM (SELECT to_jsonb(t) AS row_value FROM {} t) rows"
                ).format(sql.Identifier(schema, table)))
                state[schema + "." + table] = cursor.fetchone()[0]
    encoded = json.dumps(state, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    emit("business_snapshot", label=label, sha256=digest,
         row_counts={name: len(rows) for name, rows in state.items()}, rows=state)
    return encoded, digest


class AirflowAPI:
    def __init__(self):
        self.session = requests.Session()
        # The log endpoint otherwise prefers text/plain for requests' */*.
        self.session.headers["Accept"] = "application/json"
        self.session.auth = (
            os.environ["AIRFLOW_ADMIN_USER"], os.environ["AIRFLOW_ADMIN_PASSWORD"],
        )
        self.base = "http://127.0.0.1:8080/api/v1"

    def request(self, method, path, payload=None, record=True):
        response = self.session.request(method, self.base + path, json=payload, timeout=30)
        if record:
            emit("airflow_api", method=method, path=path, status=response.status_code)
        require(response.ok, "Airflow API returned a non-success HTTP status")
        return response.json() if response.content else None

    def run_path(self, run_id):
        return "/dags/" + DAG_ID + "/dagRuns/" + quote(run_id, safe="")

    def trigger(self, suffix, conf=None):
        run_id = "m1-t04-" + suffix + "-" + uuid.uuid4().hex
        body = self.request("POST", "/dags/" + DAG_ID + "/dagRuns", {
            "dag_run_id": run_id, "conf": conf or {},
        })
        require(body["dag_run_id"] == run_id, "API trigger run ID differs")
        emit("manual_trigger", dag_run_id=run_id, conf=conf or {}, state=body["state"])
        return run_id

    def wait(self, run_id, expected):
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            body = self.request("GET", self.run_path(run_id), record=False)
            if body["state"] in ("success", "failed"):
                tasks = self.request("GET", self.run_path(run_id) + "/taskInstances")
                states = {task["task_id"]: {
                    name: task.get(name) for name in (
                        "state", "try_number", "operator", "hostname", "pid",
                        "start_date", "end_date", "duration",
                    )
                } for task in tasks["task_instances"]}
                emit("dag_run_terminal", dag_run_id=run_id, state=body["state"], tasks=states)
                require(set(states) == set(TASK_IDS), "Unexpected business task set")
                for task in states.values():
                    require(task["operator"] == "ExternalPythonOperator", "Unexpected task operator")
                require(body["state"] == expected, "Unexpected terminal DagRun state")
                if expected == "success":
                    require(all(task["state"] == "success" for task in states.values()),
                            "Successful DagRun lacks three successful tasks")
                else:
                    require(states["ingest_sources"]["state"] == "success"
                            and states["capture_snapshot"]["state"] == "success"
                            and states["reconcile_snapshot"]["state"] == "failed",
                            "Controlled failure was not isolated to reconciliation")
                return states
            time.sleep(3)
        raise CheckFailed("Native scheduler run exceeded 600 second timeout")

    def logs(self, run_id, task_id, attempt=1):
        path = self.run_path(run_id) + "/taskInstances/" + task_id + "/logs/" + str(attempt)
        body = self.request("GET", path + "?full_content=true")
        content = body.get("content", "") if isinstance(body, dict) else str(body)
        require(bool(content), "Task execution log was empty")
        emit("task_log", dag_run_id=run_id, task_id=task_id, attempt=attempt, content=content)
        return content

    def cutoff(self, run_id):
        entries = []
        for task_id in TASK_IDS:
            path = self.run_path(run_id) + "/taskInstances/" + task_id + "/xcomEntries"
            body = self.request("GET", path)
            entries.extend((task_id, item["key"]) for item in body["xcom_entries"])
        require(entries == [("capture_snapshot", "return_value")],
                "XCom contains data other than the capture cutoff")
        body = self.request("GET", self.run_path(run_id)
                            + "/taskInstances/capture_snapshot/xcomEntries/return_value?stringify=false")
        value = body["value"]
        require(type(value) is int and 0 <= value <= 9223372036854775807,
                "Cutoff XCom was not a nonnegative BIGINT")
        emit("cutoff_xcom", dag_run_id=run_id, snapshot_cutoff=value, entries=entries)
        return value


def verify_parse_and_runtime(api):
    from airflow.configuration import conf
    from airflow.models import DagBag

    bag = DagBag(include_examples=False)
    emit("dag_parse", dag_ids=sorted(bag.dags), import_errors=bag.import_errors)
    require(not bag.import_errors and DAG_ID in bag.dags, "DAG parse/import failed")
    dag = bag.dags[DAG_ID]
    require(dag.schedule_interval is None and not dag.catchup and dag.max_active_runs == 1,
            "Manual-only DAG settings differ")
    require(set(dag.task_ids) == set(TASK_IDS), "Parsed DAG task set differs")
    require(dag.get_task("ingest_sources").downstream_task_ids == {"capture_snapshot"}
            and dag.get_task("capture_snapshot").downstream_task_ids == {"reconcile_snapshot"}
            and not dag.get_task("reconcile_snapshot").downstream_task_ids,
            "Business task dependencies differ")
    require(not dag.get_task("ingest_sources").do_xcom_push
            and not dag.get_task("reconcile_snapshot").do_xcom_push
            and dag.get_task("capture_snapshot").do_xcom_push,
            "DAG XCom settings differ")
    actual = {"executor": conf.get("core", "executor"),
              "parallelism": conf.getint("core", "parallelism"),
              "max_active_tasks_per_dag": conf.getint("core", "max_active_tasks_per_dag"),
              "max_active_runs_per_dag": conf.getint("core", "max_active_runs_per_dag")}
    emit("airflow_runtime", **actual)
    require(actual == {"executor": "LocalExecutor", "parallelism": 2,
                       "max_active_tasks_per_dag": 2, "max_active_runs_per_dag": 1},
            "Existing LocalExecutor limits changed")
    health_response = api.session.get("http://127.0.0.1:8080/health", timeout=30)
    require(health_response.ok, "Airflow health request failed")
    health = health_response.json()
    emit("airflow_health", health=health)
    require(health["metadatabase"]["status"] == "healthy"
            and health["scheduler"]["status"] == "healthy", "Airflow scheduler health failed")
    errors = api.request("GET", "/importErrors?limit=100")
    require(errors["total_entries"] == 0, "Scheduler reports DAG import errors")
    tables = query("SELECT schemaname, tablename FROM pg_tables "
                   "WHERE schemaname IN ('bronze','core') ORDER BY 1,2")
    emit("retail_tables", tables=tables)
    require(set(tables) == set(TABLES) | {("core", "evaluation_runs")},
            "Retail no longer contains exactly eight approved tables")


def verify_inputs():
    """An independent byte/identity oracle, without importing ingestion code."""
    fixture_dir = Path("/workspace/fixtures/reconciliation")
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    require(manifest["synthetic"] is True, "Fixture manifest is not marked synthetic")
    counts = {"orders": 4, "payments": 5, "refunds": 1}
    observed = []
    for item in manifest["inputs"]:
        source, reference = item["source"], item["path"]
        content = (fixture_dir / reference).read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        identity = json.dumps(["retail-jsonl-v1", source, reference, digest],
                              ensure_ascii=False, separators=(",", ":"))
        batch_id = hashlib.sha256(identity.encode()).hexdigest()
        rows = query("SELECT source_name, content_digest, input_reference, row_count, "
                     "scope_complete, status, batch_sequence FROM bronze.input_batches WHERE batch_id=%s",
                     (batch_id,))
        require(len(rows) == 1, "Manifest batch was not persisted exactly once")
        row = rows[0]
        require(row[:6] == (source, digest, reference, counts[source], True, "COMMITTED"),
                "Committed manifest batch metadata differs")
        lines = content.decode("utf-8").splitlines(keepends=True)
        raw = query(sql.SQL("SELECT source_row_number, raw_payload FROM {} "
                            "WHERE batch_id=%s ORDER BY source_row_number").format(
                                sql.Identifier("bronze", source + "_source")), (batch_id,))
        require(raw == list(enumerate(lines, 1)), "Persisted raw occurrences differ from fixture bytes")
        observed.append({"source_name": source, "batch_id": batch_id, "row_count": row[3],
                         "scope_complete": row[4], "status": row[5], "batch_sequence": row[6]})
    require({item["source_name"] for item in observed} == set(counts), "Three source manifest changed")
    emit("manifest_batches", batches=observed)


def evaluation_key(cutoff):
    # Fixed expected context encoding; do not call implementation Context/evaluate.
    data = json.dumps(["retail-evaluation-v1", cutoff, AS_OF, RULES], separators=(",", ":"))
    return hashlib.sha256(data.encode()).hexdigest()


def verify_scenarios(key, cutoff):
    statement = """
        SELECT r.order_id,r.expected_cents,r.payments_cents,r.refunds_cents,r.net_cents,
               r.overall_status,ARRAY(SELECT f.reason_code FROM core.reconciliation_findings f
                   WHERE f.evaluation_key=r.evaluation_key AND f.order_id=r.order_id ORDER BY f.reason_code)
        FROM core.reconciliation_results r WHERE r.evaluation_key=%s AND r.order_id=ANY(%s)
        ORDER BY r.order_id
    """
    actual = {row[0]: tuple(row[1:]) for row in query(statement, (key, list(EXPECTED)))}
    emit("scenario_expected_actual", evaluation_key=key, expected=EXPECTED, actual=actual,
         query="persisted canonical results LEFT correlated findings; filter three synthetic order IDs")
    require(actual == EXPECTED, "F01/F03/F08 persisted business results differ from independent oracle")
    contexts = query("SELECT DISTINCT snapshot_cutoff,"
                     "to_char(evaluation_as_of AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),"
                     "rules_version,status "
                     "FROM core.evaluation_runs WHERE evaluation_key=%s", (key,))
    require(contexts and all(row[:3] == (cutoff, AS_OF, RULES) for row in contexts)
            and any(row[3] == "SUCCEEDED" for row in contexts),
            "Evaluation context or attempt status differs")
    emit("evaluation_context", evaluation_key=key, contexts=contexts)


def main():
    api = AirflowAPI()
    verify_parse_and_runtime(api)
    dag_path = "/dags/" + DAG_ID
    initial = api.request("GET", dag_path)
    initially_paused = initial["is_paused"]
    active = api.request("GET", dag_path + "/dagRuns?state=running&limit=100")
    queued = api.request("GET", dag_path + "/dagRuns?state=queued&limit=100")
    require(active["total_entries"] == 0 and queued["total_entries"] == 0,
            "Another active DagRun would interfere with evidence")
    runs = []
    summary = None
    try:
        api.request("PATCH", dag_path, {"is_paused": False})
        first = api.trigger("initial")
        states = api.wait(first, "success")
        runs.append({"dag_run_id": first, "state": "success", "tasks": states})
        cutoff = api.cutoff(first)
        max_committed = query("SELECT coalesce(max(batch_sequence),0) FROM bronze.input_batches "
                              "WHERE status='COMMITTED'")[0][0]
        emit("captured_maximum", xcom_cutoff=cutoff, actual_max_committed=max_committed)
        require(cutoff == max_committed, "Captured cutoff differs from maximum committed batch sequence")
        key = evaluation_key(cutoff)
        for task in TASK_IDS:
            content = api.logs(first, task)
            if task == "reconcile_snapshot":
                require(key in content and ("PUBLISHED" in content or "UNCHANGED" in content),
                        "Initial reconciliation log lacks the expected evaluation key/action")
        verify_inputs()
        verify_scenarios(key, cutoff)
        original, digest = snapshot("after-initial-success")
        for number in (1, 2):
            run_id = api.trigger("replay-" + str(number))
            states = api.wait(run_id, "success")
            require(api.cutoff(run_id) == cutoff, "Replay cutoff changed")
            for task in TASK_IDS:
                content = api.logs(run_id, task)
                if task == "reconcile_snapshot":
                    require("UNCHANGED" in content and key in content,
                            "Replay reconciliation log lacks unchanged action and stable key")
            current, current_digest = snapshot("after-replay-" + str(number))
            require(current == original, "Replay changed persisted Bronze/canonical/DQ state")
            emit("replay_invariance", replay=number, before_sha256=digest,
                 after_sha256=current_digest, equal=True)
            runs.append({"dag_run_id": run_id, "state": "success", "tasks": states})
        attempts_before_failure = query(
            "SELECT run_id,status FROM core.evaluation_runs WHERE evaluation_key=%s ORDER BY run_id", (key,))
        failure_id = api.trigger("controlled-failure", {"fail_reconcile_once": True})
        failed_states = api.wait(failure_id, "failed")
        failure_log = api.logs(failure_id, "reconcile_snapshot")
        require("Controlled" in failure_log or "controlled" in failure_log,
                "Controlled failure diagnosis is absent from task log")
        require(api.cutoff(failure_id) == cutoff, "Failed run cutoff changed")
        failed_snapshot, _ = snapshot("after-controlled-failure")
        require(failed_snapshot == original, "Controlled failure changed persisted business state")
        attempts_after_failure = query(
            "SELECT run_id,status FROM core.evaluation_runs WHERE evaluation_key=%s ORDER BY run_id", (key,))
        require(attempts_after_failure == attempts_before_failure,
                "Pre-evaluate failure unexpectedly changed Retail evaluation attempts")
        emit("failure_attempt_invariance", before=attempts_before_failure, after=attempts_after_failure,
             equal=True)
        clear_body = {
            "dag_run_id": failure_id, "task_ids": ["reconcile_snapshot"], "dry_run": False,
            "only_failed": True, "reset_dag_runs": True, "include_upstream": False,
            "include_downstream": False, "include_future": False, "include_past": False,
        }
        cleared = api.request("POST", dag_path + "/clearTaskInstances", clear_body)
        emit("clear_failed_task", request=clear_body, response=cleared)
        require(len(cleared["task_instances"]) == 1
                and cleared["task_instances"][0]["task_id"] == "reconcile_snapshot"
                and cleared["task_instances"][0]["dag_run_id"] == failure_id
                and cleared["task_instances"][0]["dag_id"] == DAG_ID,
                "Clear operation did not select exactly the failed reconciliation task")
        retried = api.wait(failure_id, "success")
        require(api.cutoff(failure_id) == cutoff, "Retry replaced the captured cutoff")
        for task in ("ingest_sources", "capture_snapshot"):
            require(retried[task]["try_number"] == failed_states[task]["try_number"] == 1,
                    "Retry reran an upstream task")
        require(failed_states["reconcile_snapshot"]["try_number"] == 1
                and retried["reconcile_snapshot"]["try_number"] == 2,
                "Reconciliation task attempt did not advance from one to two")
        retry_log = api.logs(failure_id, "reconcile_snapshot", 2)
        require("UNCHANGED" in retry_log and key in retry_log, "Retry did not reuse canonical evaluation")
        attempts_after_retry = query(
            "SELECT run_id,status FROM core.evaluation_runs WHERE evaluation_key=%s ORDER BY run_id", (key,))
        new_attempts = set(attempts_after_retry) - set(attempts_before_failure)
        require(len(attempts_after_retry) == len(attempts_before_failure) + 1
                and set(attempts_before_failure).issubset(attempts_after_retry)
                and len(new_attempts) == 1 and next(iter(new_attempts))[1] == "SUCCEEDED",
                "Safe retry did not append exactly one successful Retail evaluation attempt")
        retried_snapshot, retried_digest = snapshot("after-safe-retry")
        require(retried_snapshot == original, "Safe retry changed persisted business state")
        verify_scenarios(key, cutoff)
        emit("controlled_failure_retry", dag_run_id=failure_id, failed_tasks=failed_states,
             retried_tasks=retried, original_sha256=digest, final_sha256=retried_digest,
             business_state_equal=True, retail_attempts_before=len(attempts_before_failure),
             retail_attempts_after_failure=len(attempts_after_failure),
             retail_attempts_after_retry=len(attempts_after_retry),
             scope="Orchestration failure before evaluate; business rollback is a separate T03 regression")
        summary = {"task": "M1-T04", "status": "PASS", "evaluation_key": key,
                   "snapshot_cutoff": cutoff, "evaluation_as_of": AS_OF, "rules_version": RULES,
                   "successful_runs": runs, "failure_retry_run_id": failure_id,
                   "failure_state": failed_states, "retry_state": retried,
                   "business_snapshot_sha256": digest,
                   "checks": ["T04-01", "T04-02", "T04-03", "T04-04", "T04-05",
                              "T04-06", "T04-07", "T04-08", "T04-09"],
                   "limitations": "T04-10 SQL report and T04-11/T04-12 regressions recorded separately"}
    finally:
        api.request("PATCH", dag_path, {"is_paused": initially_paused})
        emit("dag_pause_restored", is_paused=initially_paused)
    print("M1_T04_SUMMARY " + json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Never print connection/library exception text or authenticated requests.
        emit("acceptance_failure", error_type=type(error).__name__,
             check=str(error) if isinstance(error, CheckFailed) else "See preceding nonsensitive evidence")
        sys.exit(1)
