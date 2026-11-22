"""Manual dbt reconciliation orchestration and atomic Core publication.

Business state and monetary calculations belong solely to the ephemeral dbt SQL
model. This module validates evaluation context and the publication protocol; it
does not calculate expected, paid, refunded or net amounts.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Callable
import uuid

from psycopg2 import extensions
from psycopg2.extras import Json

from retail_ingestion import connect_from_env, parse_timestamp


RULES_VERSION = "m1-thin-v1"
BIGINT_MAX = 9223372036854775807
BIGINT_MIN = -9223372036854775808
MODEL_NAME = "thin_reconciliation"
CONTEXT_MARKER = "-- retail-evaluation-context: "
RESULT_COLUMNS = (
    "order_id", "expected_cents", "payments_cents", "refunds_cents",
    "net_cents", "overall_status",
)
MODEL_COLUMNS = RESULT_COLUMNS + ("finding_reason", "evidence_refs", "details")
OUTPUT_STATUSES = frozenset({
    "MATCHED", "PENDING", "EXCEPTION", "UNDETERMINED", "NOT_APPLICABLE",
})


class EvaluationError(ValueError):
    """Invalid context, compilation failure or invalid SQL publication output."""


class EvaluationConflictError(EvaluationError):
    """An evaluation key cannot safely represent the proposed context/output."""


@dataclass(frozen=True)
class Context:
    """Explicit, canonically encoded context; equivalent UTC instants share a key."""

    snapshot_cutoff: int
    evaluation_as_of: datetime | str
    rules_version: str = RULES_VERSION

    def __post_init__(self) -> None:
        if type(self.snapshot_cutoff) is not int or not 0 <= self.snapshot_cutoff <= BIGINT_MAX:
            raise EvaluationError("snapshot_cutoff must be a nonnegative BIGINT integer")
        if self.rules_version != RULES_VERSION:
            raise EvaluationError("Unsupported reconciliation rules version")
        if isinstance(self.evaluation_as_of, str):
            value = parse_timestamp(self.evaluation_as_of)
        elif isinstance(self.evaluation_as_of, datetime):
            try:
                value = (self.evaluation_as_of.astimezone(timezone.utc)
                         if self.evaluation_as_of.utcoffset() is not None else None)
            except (ValueError, OverflowError):
                value = None
        else:
            value = None
        if value is None:
            raise EvaluationError("evaluation_as_of requires an explicit offset and microsecond precision")
        object.__setattr__(self, "evaluation_as_of", value)

    @property
    def canonical_as_of(self) -> str:
        return self.evaluation_as_of.isoformat(timespec="microseconds").replace("+00:00", "Z")

    @property
    def evaluation_key(self) -> str:
        encoded = json.dumps(
            ["retail-evaluation-v1", self.snapshot_cutoff, self.canonical_as_of, self.rules_version],
            ensure_ascii=False, separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @property
    def dbt_vars(self) -> dict[str, Any]:
        return {
            "snapshot_cutoff": self.snapshot_cutoff,
            "evaluation_as_of": self.canonical_as_of,
            "rules_version": self.rules_version,
        }


def compile_model(context: Context) -> str:
    """Compile one ephemeral model into a private, disposable target directory.

    The context marker prevents accidental reuse of SQL compiled for a different
    evaluation. There is no CLI option to submit arbitrary precompiled SQL.
    """
    project = Path(__file__).resolve().parents[1] / "dbt"
    with tempfile.TemporaryDirectory(prefix="retail-dbt-evaluation-") as folder:
        temporary = Path(folder)
        target = temporary / "target"
        environment = os.environ.copy()
        environment.update({
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DBT_PARTIAL_PARSE": "false",
            "DBT_LOG_PATH": str(temporary / "logs"),
            "DBT_TARGET_PATH": str(target),
            "DBT_USE_COLORS": "false",
        })
        command = [
            "dbt", "compile", "--project-dir", str(project),
            "--profiles-dir", str(project), "--select", MODEL_NAME,
            "--vars", json.dumps(context.dbt_vars, separators=(",", ":")),
            "--target-path", str(target), "--no-introspect",
        ]
        try:
            completed = subprocess.run(
                command, cwd=project, env=environment,
                text=True, capture_output=True, check=False,
            )
        except (OSError, UnicodeError) as error:
            raise EvaluationError("dbt compilation could not execute") from error
        if completed.returncode != 0:
            # dbt diagnostic text can contain profile credentials; do not expose it.
            raise EvaluationError("dbt compilation returned a nonzero exit")
        compiled = target / "compiled" / "retail_foundation" / "models" / (MODEL_NAME + ".sql")
        if not compiled.is_file():
            raise EvaluationError("dbt did not produce the expected compiled model")
        sql_text = compiled.read_text(encoding="utf-8").strip()
        if not sql_text:
            raise EvaluationError("dbt produced an empty compiled model")
        return CONTEXT_MARKER + context.evaluation_key + "\n" + sql_text


def _validate_model_rows(cursor: Any) -> list[dict[str, Any]]:
    if cursor.description is None:
        raise EvaluationError("Compiled model did not return a result set")
    names = tuple(column.name for column in cursor.description)
    if names != MODEL_COLUMNS:
        raise EvaluationError("Compiled model output does not match the publication contract")
    rows = []
    order_ids = set()
    for values in cursor.fetchall():
        row = dict(zip(names, values))
        order_id = row["order_id"]
        if not isinstance(order_id, str) or not order_id or order_id in order_ids:
            raise EvaluationError("Compiled model must return one unique nonempty order identity")
        order_ids.add(order_id)
        for name in RESULT_COLUMNS[1:5]:
            value = row[name]
            if value is not None and (type(value) is not int or not BIGINT_MIN <= value <= BIGINT_MAX):
                raise EvaluationError("Compiled monetary output must be nullable BIGINT cents")
        if row["overall_status"] not in OUTPUT_STATUSES:
            raise EvaluationError("Compiled model returned an unsupported status")
        if row["finding_reason"] not in (None, "MISSING_PAYMENT"):
            raise EvaluationError("Compiled model returned a finding outside this thin slice")
        if (row["overall_status"] == "EXCEPTION") != (row["finding_reason"] is not None):
            raise EvaluationError("Compiled status/finding publication contract is inconsistent")
        if not isinstance(row["evidence_refs"], list) or not isinstance(row["details"], dict):
            raise EvaluationError("Compiled lineage must be a JSON array and details a JSON object")
        rows.append(row)
    return sorted(rows, key=lambda row: row["order_id"])


def _canonical_payload(rows: list[dict[str, Any]]) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    results = [tuple(row[column] for column in RESULT_COLUMNS) for row in rows]
    findings = [
        (row["order_id"], row["finding_reason"], row["evidence_refs"], row["details"])
        for row in rows if row["finding_reason"] is not None
    ]
    return results, findings


def _publish_rows(cursor: Any, evaluation_key: str, rows: list[dict[str, Any]]) -> None:
    """Insert SQL-calculated results/findings without recomputing business state."""
    for row in rows:
        cursor.execute(
            "INSERT INTO core.reconciliation_results "
            "(evaluation_key,order_id,expected_cents,payments_cents,refunds_cents,net_cents,overall_status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (evaluation_key,) + tuple(row[column] for column in RESULT_COLUMNS),
        )
        if row["finding_reason"] is not None:
            cursor.execute(
                "INSERT INTO core.reconciliation_findings "
                "(evaluation_key,order_id,reason_code,evidence_refs,details) VALUES (%s,%s,%s,%s,%s)",
                (evaluation_key, row["order_id"], row["finding_reason"],
                 Json(row["evidence_refs"]), Json(row["details"])),
            )


def _register_attempt(control: Any, context: Context, run_id: str) -> bool:
    """Reject context reuse before creating an attempt with contradictory metadata."""
    with control:
        with control.cursor() as cursor:
            cursor.execute(
                "SELECT snapshot_cutoff,evaluation_as_of,rules_version,status "
                "FROM core.evaluation_runs WHERE evaluation_key=%s",
                (context.evaluation_key,),
            )
            existing = cursor.fetchall()
            expected = (context.snapshot_cutoff, context.evaluation_as_of, context.rules_version)
            if any(row[:3] != expected or row[3] not in ("RUNNING", "FAILED", "SUCCEEDED") for row in existing):
                raise EvaluationConflictError("Evaluation key already represents conflicting context or state")
            previous_success = any(row[3] == "SUCCEEDED" for row in existing)
            cursor.execute(
                "INSERT INTO core.evaluation_runs "
                "(run_id,evaluation_key,snapshot_cutoff,evaluation_as_of,rules_version,status) "
                "VALUES (%s,%s,%s,%s,%s,'RUNNING')",
                (run_id, context.evaluation_key, context.snapshot_cutoff,
                 context.evaluation_as_of, context.rules_version),
            )
    return previous_success


def evaluate(
    context: Context, *, compiled_sql: str | None = None,
    connection_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Publish or verify one complete SQL result set in a consistent source snapshot.

    A session advisory lock is acquired on a control connection *before* creating
    the worker's REPEATABLE READ snapshot. This avoids stale snapshots after a
    concurrent identical writer finishes. The control connection preserves the
    attempt/failure record; canonical rows and successful status commit together.
    """
    if not isinstance(context, Context):
        raise EvaluationError("An explicit validated Context is required")
    factory = connection_factory or connect_from_env
    run_id = str(uuid.uuid4())
    control = factory()
    worker = None
    registered = False
    lock_key = int.from_bytes(bytes.fromhex(context.evaluation_key)[:8], "big", signed=True)
    try:
        if control.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE:
            raise EvaluationError("Evaluation requires an idle control connection")
        control.autocommit = True
        with control.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_lock(%s)", (lock_key,))
        control.autocommit = False
        previous_success = _register_attempt(control, context, run_id)
        registered = True
        query = compiled_sql if compiled_sql is not None else compile_model(context)
        marker = CONTEXT_MARKER + context.evaluation_key + "\n"
        if not isinstance(query, str) or not query.startswith(marker):
            raise EvaluationError("Compiled SQL belongs to a different evaluation context")
        worker = factory()
        worker.set_session(isolation_level="REPEATABLE READ", readonly=False, autocommit=False)
        with worker:
            with worker.cursor() as cursor:
                cursor.execute("SET LOCAL TIME ZONE 'UTC'")
                cursor.execute("SELECT COALESCE(max(batch_sequence),0) FROM bronze.input_batches WHERE status='COMMITTED'")
                maximum_sequence = cursor.fetchone()[0]
                if context.snapshot_cutoff > maximum_sequence:
                    raise EvaluationError("snapshot_cutoff exceeds the committed source sequence")
                cursor.execute(query)
                rows = _validate_model_rows(cursor)
                expected_results, expected_findings = _canonical_payload(rows)
                cursor.execute(
                    "SELECT order_id,expected_cents,payments_cents,refunds_cents,net_cents,overall_status "
                    "FROM core.reconciliation_results WHERE evaluation_key=%s ORDER BY order_id",
                    (context.evaluation_key,),
                )
                persisted_results = sorted(cursor.fetchall(), key=lambda row: row[0])
                cursor.execute(
                    "SELECT order_id,reason_code,evidence_refs,details FROM core.reconciliation_findings "
                    "WHERE evaluation_key=%s ORDER BY order_id,reason_code",
                    (context.evaluation_key,),
                )
                persisted_findings = sorted(cursor.fetchall(), key=lambda row: (row[0], row[1]))
                if previous_success:
                    if persisted_results != expected_results or persisted_findings != expected_findings:
                        raise EvaluationConflictError("Existing canonical results conflict with the evaluated snapshot")
                    action = "UNCHANGED"
                else:
                    if persisted_results or persisted_findings:
                        raise EvaluationConflictError("Canonical rows exist without a successful publication marker")
                    _publish_rows(cursor, context.evaluation_key, rows)
                    action = "PUBLISHED"
                cursor.execute(
                    "UPDATE core.evaluation_runs SET status='SUCCEEDED', finished_at=clock_timestamp(),error_details=NULL "
                    "WHERE run_id=%s AND status='RUNNING'", (run_id,),
                )
                if cursor.rowcount != 1:
                    raise EvaluationConflictError("Attempt metadata changed during publication")
        return {
            "run_id": run_id, "evaluation_key": context.evaluation_key,
            "snapshot_cutoff": context.snapshot_cutoff,
            "evaluation_as_of": context.canonical_as_of,
            "rules_version": context.rules_version,
            "action": action, "results": rows,
        }
    except Exception as error:
        if worker is not None:
            worker.rollback()
        if registered:
            with control:
                with control.cursor() as cursor:
                    cursor.execute(
                        "UPDATE core.evaluation_runs SET status='FAILED',finished_at=clock_timestamp(),error_details=%s "
                        "WHERE run_id=%s AND status='RUNNING'",
                        (json.dumps({"error_type": type(error).__name__}, sort_keys=True), run_id),
                    )
        raise
    finally:
        if worker is not None:
            worker.close()
        # Closing releases the session advisory lock even after an exception.
        control.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate the bounded synthetic reconciliation thin slice")
    parser.add_argument("--snapshot-cutoff", type=int, required=True)
    parser.add_argument("--evaluation-as-of", required=True)
    parser.add_argument("--rules-version", required=True)
    args = parser.parse_args(argv)
    try:
        context = Context(args.snapshot_cutoff, args.evaluation_as_of, args.rules_version)
        print(json.dumps(evaluate(context), ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as error:
        # SQL/compiler errors may carry credentials, row data or local paths.
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
