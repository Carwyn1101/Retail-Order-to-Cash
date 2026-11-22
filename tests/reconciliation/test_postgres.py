"""Native M1-T03 reconciliation assertions with independent expected values.

Tests use real PostgreSQL and dbt compilation. Fault/snapshot hooks inject real
SQL into isolated synthetic records; no database mock stands in for execution.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import psycopg2
from psycopg2 import extensions, sql

import retail_ingestion as ingestion
import retail_reconciliation as reconciliation


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "reconciliation"
SOURCES = ("orders", "payments", "refunds")
AFTER_DUE = "2026-02-03T00:00:00Z"
EXPECTED_TABLES = [
    ("bronze", "input_batches"), ("bronze", "orders_source"),
    ("bronze", "payments_source"), ("bronze", "refunds_source"),
    ("core", "dq_findings"), ("core", "evaluation_runs"),
    ("core", "reconciliation_findings"), ("core", "reconciliation_results"),
]


class NativeReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.prefix = "syn-t03-native-" + uuid.uuid4().hex
        self.owned_batches = []
        self.owned_keys = []
        self.compiled = {}
        self.writer = ingestion.connect_from_env()
        self.reader = ingestion.connect_from_env()
        self.reader.autocommit = True

    def tearDown(self):
        try:
            self.writer.rollback()
            with self.writer:
                with self.writer.cursor() as cursor:
                    if self.owned_keys:
                        for name in ("reconciliation_findings", "reconciliation_results", "evaluation_runs"):
                            cursor.execute(sql.SQL("DELETE FROM {} WHERE evaluation_key=ANY(%s)").format(
                                sql.Identifier("core", name)), (self.owned_keys,))
                    if self.owned_batches:
                        for source in SOURCES:
                            cursor.execute(sql.SQL("DELETE FROM {} WHERE batch_id=ANY(%s)").format(
                                sql.Identifier("bronze", source + "_source")), (self.owned_batches,))
                        cursor.execute("DELETE FROM bronze.input_batches WHERE batch_id=ANY(%s)", (self.owned_batches,))
            if self.owned_keys:
                self.assertEqual(self.query("SELECT count(*) FROM core.evaluation_runs WHERE evaluation_key=ANY(%s)", (self.owned_keys,)), [(0,)])
            if self.owned_batches:
                self.assertEqual(self.query("SELECT count(*) FROM bronze.input_batches WHERE batch_id=ANY(%s)", (self.owned_batches,)), [(0,)])
        finally:
            self.writer.close()
            self.reader.close()

    def query(self, statement, parameters=()):
        with self.reader.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.fetchall()

    def oid(self, suffix):
        return self.prefix + "-" + suffix

    def ingest(self, source, content, suffix=None, complete=True):
        if not isinstance(content, bytes):
            content = ("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in content)).encode("utf-8")
        batch = ingestion.BatchInput(source, "synthetic-t03/" + self.prefix + "/" + (suffix or source + ".jsonl"), complete, content)
        self.owned_batches.append(batch.batch_id)
        result = ingestion.ingest_batch(self.writer, batch)
        self.assertEqual(result["action"], "COMMITTED")
        return batch

    def seed(self):
        batches = {}
        for source in SOURCES:
            content = (FIXTURES / (source + ".jsonl")).read_bytes().replace(b"syn-t03-", (self.prefix + "-").encode())
            batches[source] = self.ingest(source, content)
        return batches

    def context(self, as_of=AFTER_DUE, cutoff=None):
        if cutoff is None:
            cutoff = self.query("SELECT coalesce(max(batch_sequence),0) FROM bronze.input_batches WHERE status='COMMITTED'")[0][0]
        context = reconciliation.Context(cutoff, as_of, "m1-thin-v1")
        if context.evaluation_key not in self.owned_keys:
            # Never take ownership of an unrelated historical evaluation.
            self.assertEqual(self.query("SELECT count(*) FROM core.evaluation_runs WHERE evaluation_key=%s", (context.evaluation_key,)), [(0,)])
            self.owned_keys.append(context.evaluation_key)
        return context

    def evaluate(self, context, **kwargs):
        if context.evaluation_key not in self.compiled:
            self.compiled[context.evaluation_key] = reconciliation.compile_model(context)
        return reconciliation.evaluate(context, compiled_sql=self.compiled[context.evaluation_key], **kwargs)

    def result(self, context, suffix):
        rows = self.query(
            "SELECT expected_cents,payments_cents,refunds_cents,net_cents,overall_status "
            "FROM core.reconciliation_results WHERE evaluation_key=%s AND order_id=%s",
            (context.evaluation_key, self.oid(suffix)),
        )
        self.assertEqual(len(rows), 1)
        return rows[0]

    def reasons(self, context, suffix):
        return self.query("SELECT reason_code FROM core.reconciliation_findings WHERE evaluation_key=%s AND order_id=%s ORDER BY reason_code", (context.evaluation_key, self.oid(suffix)))

    def canonical_snapshot(self, context):
        return [self.query(sql.SQL("SELECT * FROM {} WHERE evaluation_key=%s ORDER BY 1,2").format(sql.Identifier("core", name)), (context.evaluation_key,))
                for name in ("reconciliation_results", "reconciliation_findings")]

    def order(self, suffix, **changes):
        row = {"synthetic": True, "source_record_id": self.oid("order-record-" + suffix),
               "order_id": self.oid(suffix), "source_version": "opaque-v1", "event_at": "2026-02-01T00:00:00Z",
               "source_updated_at": "2026-02-01T00:05:00Z", "order_status": "CONFIRMED", "amount_raw": "100.00",
               "currency": "USD", "payment_due_at": "2026-02-02T00:00:00Z"}
        row.update(changes)
        return row

    def payment(self, suffix, **changes):
        row = {"synthetic": True, "source_record_id": self.oid("payment-record-" + suffix),
               "payment_id": self.oid("payment-" + suffix), "order_id": self.oid(suffix),
               "external_charge_ref": self.oid("charge-" + suffix), "source_version": "opaque-v1",
               "event_at": "2026-02-01T02:00:00Z", "source_updated_at": "2026-02-01T02:05:00Z",
               "financial_effective_at": "2026-02-01T02:00:00Z", "payment_status": "SUCCEEDED",
               "amount_raw": "100.00", "currency": "USD"}
        row.update(changes)
        return row

    def refund(self, suffix, **changes):
        row = {"synthetic": True, "source_record_id": self.oid("refund-record-" + suffix),
               "refund_id": self.oid("refund-" + suffix), "order_id": self.oid(suffix),
               "payment_id": self.oid("payment-" + suffix), "source_version": "opaque-v1",
               "event_at": "2026-02-01T12:00:00Z", "source_updated_at": "2026-02-01T12:05:00Z",
               "financial_effective_at": "2026-02-01T12:00:00Z", "refund_status": "COMPLETED",
               "amount_raw": "20.00", "currency": "USD"}
        row.update(changes)
        return row

    def test_compiled_dbt_native_f01_f03_f08_independent_oracle(self):
        self.seed()
        context = self.context()
        outcome = self.evaluate(context)
        self.assertEqual(outcome["action"], "PUBLISHED")
        expected = {"f01": (10000, 10000, 0, 10000, "MATCHED"),
                    "f03": (4500, 0, 0, 0, "EXCEPTION"),
                    "f08": (8000, 8000, 2000, 6000, "MATCHED")}
        observed = {suffix: self.result(context, suffix) for suffix in expected}
        self.assertEqual(observed, expected)
        self.assertEqual(self.reasons(context, "f01"), [])
        self.assertEqual(self.reasons(context, "f03"), [("MISSING_PAYMENT",)])
        self.assertEqual(self.reasons(context, "f08"), [])
        self.assertIn("-- retail-evaluation-context: " + context.evaluation_key, self.compiled[context.evaluation_key])
        self.assertEqual(self.query("SELECT schemaname,tablename FROM pg_tables WHERE schemaname !~ '^pg_' AND schemaname<>'information_schema' ORDER BY 1,2"), EXPECTED_TABLES)
        print(f"T03-02/03/04/06 native compiled dbt oracle expected={expected}, observed={observed}; findings F01=[], F03=[MISSING_PAYMENT], F08=[]; exactly eight tables", flush=True)

    def test_missing_payment_before_and_at_due_is_pending(self):
        self.seed()
        for as_of in ("2026-02-01T23:00:00Z", "2026-02-02T00:00:00Z"):
            with self.subTest(evaluation_as_of=as_of):
                context = self.context(as_of)
                self.evaluate(context)
                observed = self.result(context, "f03")
                self.assertEqual(observed, (4500, 0, 0, 0, "PENDING"))
                self.assertEqual(self.reasons(context, "f03"), [])
                print(f"T03-05 native as_of={as_of}, expected/observed={observed}, findings=[]", flush=True)

    def test_equivalent_versions_and_charge_alias_do_not_double_count(self):
        batches = self.seed()
        context = self.context()
        self.evaluate(context)
        self.assertEqual(self.query("SELECT count(*),count(DISTINCT payment_id),count(DISTINCT external_charge_ref) FROM bronze.payments_source WHERE batch_id=%s AND order_id=%s", (batches["payments"].batch_id, self.oid("f01"))), [(3, 2, 1)])
        self.assertEqual(self.result(context, "f01"), (10000, 10000, 0, 10000, "MATCHED"))
        self.assertEqual(self.reasons(context, "f01"), [])
        print("T03-07 native F01 retains three payment occurrences/two payment IDs/one charge; P expected=10000 observed=10000", flush=True)

    def test_invalid_conflicting_and_unresolved_evidence_never_matches(self):
        self.seed()
        cases = ("bad-money", "unknown-status", "missing-effective", "future-effective", "missing-payment-id",
                 "bad-update-time", "version-conflict", "bad-refund", "orphan-refund", "order-conflict", "cross-charge-a", "cross-charge-b")
        orders = [self.order(case) for case in cases]
        orders.append(self.order("order-conflict", source_record_id=self.oid("conflicting-order-version"), source_version="opaque-v2", amount_raw="101.00"))
        payments = []
        for case in cases:
            changes = {}
            if case == "bad-money": changes["amount_raw"] = "100.001"
            if case == "unknown-status": changes["payment_status"] = "UNKNOWN"
            if case == "missing-effective": changes["financial_effective_at"] = None
            if case == "future-effective": changes["financial_effective_at"] = "2027-02-01T02:00:00Z"
            if case == "missing-payment-id": changes["payment_id"] = None
            if case == "bad-update-time": changes["source_updated_at"] = "invalid-time"
            if case.startswith("cross-charge-"): changes["external_charge_ref"] = self.oid("shared-cross-order-charge")
            payments.append(self.payment(case, **changes))
        payments.append(self.payment("version-conflict", source_record_id=self.oid("conflicting-payment-version"), source_version="opaque-v2", amount_raw="99.00"))
        self.ingest("orders", orders, suffix="invalid-orders.jsonl")
        bad_batch = self.ingest("payments", payments, suffix="invalid-payments.jsonl")
        self.ingest("refunds", [self.refund("bad-refund", refund_status="UNKNOWN"), self.refund("orphan-refund", payment_id=self.oid("nonexistent-payment"))], suffix="invalid-refunds.jsonl")
        context = self.context()
        self.evaluate(context)
        statuses = {case: self.result(context, case)[4] for case in cases}
        self.assertEqual(statuses, {case: "UNDETERMINED" for case in cases})
        self.assertEqual(self.result(context, "f01")[4], "MATCHED", "Unrelated malformed cases must not become accepted money for clean F01")
        self.assertEqual(self.query("SELECT amount_raw,amount_cents FROM bronze.payments_source WHERE batch_id=%s AND order_id=%s", (bad_batch.batch_id, self.oid("bad-money"))), [("100.001", None)])
        self.assertEqual(self.query("SELECT payment_status FROM bronze.payments_source WHERE batch_id=%s AND order_id=%s", (bad_batch.batch_id, self.oid("unknown-status"))), [("UNKNOWN",)])
        print(f"T03-08 native linked invalid/conflicting/identity cases expected/observed={statuses}; invalid raw retained; clean F01 MATCHED", flush=True)

    def test_unsupported_mismatch_full_refund_and_bigint_overflow_are_undetermined(self):
        self.seed()
        self.ingest("orders", [self.order("underpaid"), self.order("full-refund"), self.order("overflow", amount_raw="92233720368547758.07")], suffix="unsupported-orders.jsonl")
        self.ingest("payments", [self.payment("underpaid", amount_raw="90.00"), self.payment("full-refund"),
                   self.payment("overflow", amount_raw="92233720368547758.07"),
                   self.payment("overflow", payment_id=self.oid("second-overflow-payment"), external_charge_ref=self.oid("second-overflow-charge"), amount_raw="0.01")], suffix="unsupported-payments.jsonl")
        self.ingest("refunds", [self.refund("full-refund", amount_raw="100.00")], suffix="full-refund.jsonl")
        context = self.context()
        self.evaluate(context)
        for suffix in ("underpaid", "full-refund", "overflow"):
            self.assertEqual(self.result(context, suffix)[4], "UNDETERMINED")
            self.assertEqual(self.reasons(context, suffix), [])
        self.assertIsNone(self.result(context, "overflow")[1])
        self.assertIsNone(self.result(context, "overflow")[3])
        print("T03-08 native unsupported mismatch/full-refund remain UNDETERMINED with no invented finding; exact sum overflow => NULL P/N", flush=True)

    def test_two_replays_leave_all_canonical_results_and_findings_unchanged(self):
        self.seed()
        context = self.context()
        initial = self.evaluate(context)
        baseline = self.canonical_snapshot(context)
        attempts = [initial["run_id"]]
        for _ in range(2):
            replay = self.evaluate(context)
            self.assertEqual(replay["action"], "UNCHANGED")
            self.assertEqual(self.canonical_snapshot(context), baseline)
            attempts.append(replay["run_id"])
        self.assertEqual(len(set(attempts)), 3)
        self.assertEqual(self.query("SELECT status,count(*) FROM core.evaluation_runs WHERE evaluation_key=%s GROUP BY status", (context.evaluation_key,)), [("SUCCEEDED", 3)])
        print("T03-09 native initial PUBLISHED + two UNCHANGED replays; all canonical columns/findings identical; three distinct successful attempts", flush=True)

    def test_concurrent_identical_evaluations_publish_once(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading

        self.seed()
        context = self.context()
        compiled = reconciliation.compile_model(context)
        ready = threading.Barrier(2)

        def connection_factory():
            connection = ingestion.connect_from_env()
            try:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout='15s'")
                    cursor.execute("SET lock_timeout='10s'")
                connection.autocommit = False
                return connection
            except Exception:
                connection.close()
                raise

        def evaluate_once():
            ready.wait(timeout=10)
            return reconciliation.evaluate(
                context, compiled_sql=compiled, connection_factory=connection_factory,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(evaluate_once) for _ in range(2)]
            outcomes = [future.result(timeout=30) for future in futures]

        actions = sorted(outcome["action"] for outcome in outcomes)
        self.assertEqual(actions, ["PUBLISHED", "UNCHANGED"])
        self.assertEqual(len({outcome["run_id"] for outcome in outcomes}), 2)
        self.assertEqual(outcomes[0]["results"], outcomes[1]["results"])
        self.assertEqual(self.query(
            "SELECT status,count(*) FROM core.evaluation_runs WHERE evaluation_key=%s GROUP BY status",
            (context.evaluation_key,),
        ), [("SUCCEEDED", 2)])
        own_orders = [self.oid("f01"), self.oid("f03"), self.oid("f08")]
        self.assertEqual(self.query(
            "SELECT count(*) FROM core.reconciliation_results WHERE evaluation_key=%s AND order_id=ANY(%s)",
            (context.evaluation_key, own_orders),
        ), [(3,)])
        self.assertEqual(self.query(
            "SELECT count(*) FROM core.reconciliation_findings WHERE evaluation_key=%s AND order_id=ANY(%s)",
            (context.evaluation_key, own_orders),
        ), [(1,)])
        self.assertEqual(self.result(context, "f01"), (10000, 10000, 0, 10000, "MATCHED"))
        self.assertEqual(self.result(context, "f03"), (4500, 0, 0, 0, "EXCEPTION"))
        self.assertEqual(self.result(context, "f08"), (8000, 8000, 2000, 6000, "MATCHED"))
        self.assertEqual(self.reasons(context, "f01"), [])
        self.assertEqual(self.reasons(context, "f03"), [("MISSING_PAYMENT",)])
        self.assertEqual(self.reasons(context, "f08"), [])
        print(f"T03-09 native concurrent actions={actions}; two distinct SUCCEEDED attempts; canonical F01/F03/F08 rows=3, findings=1; no duplicate publication", flush=True)

    def test_empty_snapshot_replay_uses_successful_attempt_marker(self):
        # No sequence can precede cutoff zero. A unique timestamp avoids taking
        # ownership of any preexisting user's empty historical evaluation.
        nonce = uuid.uuid4().int
        as_of = datetime(2050, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=nonce % 31536000, microseconds=(nonce >> 32) % 1000000)
        context = self.context(as_of, cutoff=0)
        first = self.evaluate(context)
        self.assertEqual(first["action"], "PUBLISHED")
        self.assertEqual(first["results"], [])
        for _ in range(2):
            self.assertEqual(self.evaluate(context)["action"], "UNCHANGED")
            self.assertEqual(self.canonical_snapshot(context), [[], []])
        self.assertEqual(self.query("SELECT status,count(*) FROM core.evaluation_runs WHERE evaluation_key=%s GROUP BY status", (context.evaluation_key,)), [("SUCCEEDED", 3)])
        print("T03-09 native cutoff0 empty canonical publication replays UNCHANGED via success marker; three successful attempts/no result rows", flush=True)

    def test_unallocated_future_snapshot_cutoff_fails_without_canonical_rows(self):
        self.seed()
        maximum = self.query("SELECT max(batch_sequence) FROM bronze.input_batches WHERE status='COMMITTED'")[0][0]
        context = self.context(cutoff=maximum + 1)
        with self.assertRaises(reconciliation.EvaluationError):
            self.evaluate(context)
        self.assertEqual(self.canonical_snapshot(context), [[], []])
        self.assertEqual(self.query("SELECT status FROM core.evaluation_runs WHERE evaluation_key=%s", (context.evaluation_key,)), [("FAILED",)])
        print("T03-12 native cutoff beyond current committed maximum rejected; FAILED attempt retained, no canonical rows", flush=True)

    def test_publication_sql_failure_rolls_back_results_findings_and_marks_attempt_failed(self):
        self.seed()
        context = self.context()
        original = reconciliation._publish_rows
        observed_partial = []

        def fail_after_real_insert(cursor, key, rows):
            first = next(row for row in rows if row["order_id"] == self.oid("f03"))
            original(cursor, key, [first])
            cursor.execute("SELECT (SELECT count(*) FROM core.reconciliation_results WHERE evaluation_key=%s),(SELECT count(*) FROM core.reconciliation_findings WHERE evaluation_key=%s)", (key, key))
            observed_partial.append(cursor.fetchone())
            cursor.execute("SELECT 1 / 0")

        with patch.object(reconciliation, "_publish_rows", side_effect=fail_after_real_insert):
            with self.assertRaises(psycopg2.errors.DivisionByZero) as failure:
                self.evaluate(context)
        self.assertEqual(failure.exception.pgcode, "22012")
        self.assertEqual(observed_partial, [(1, 1)])
        self.assertEqual(self.canonical_snapshot(context), [[], []])
        failed = self.query("SELECT status,error_details FROM core.evaluation_runs WHERE evaluation_key=%s", (context.evaluation_key,))
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0][0], "FAILED")
        self.assertIn("DivisionByZero", failed[0][1])
        self.assertEqual(self.evaluate(context)["action"], "PUBLISHED")
        self.assertEqual(self.query("SELECT status FROM core.evaluation_runs WHERE evaluation_key=%s ORDER BY status", (context.evaluation_key,)), [("FAILED",), ("SUCCEEDED",)])
        print("T03-10 native SQLSTATE 22012 after 1 result + 1 finding; persisted canonical rows=0; FAILED attempt retained; retry succeeds separately", flush=True)

    def test_existing_key_conflicting_context_is_rejected_without_overwrite(self):
        self.seed()
        context = self.context()
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute("INSERT INTO core.evaluation_runs(run_id,evaluation_key,snapshot_cutoff,evaluation_as_of,rules_version,status) VALUES (%s,%s,%s,%s,%s,'FAILED')", (self.prefix + "-conflict", context.evaluation_key, context.snapshot_cutoff, context.evaluation_as_of + timedelta(seconds=1), context.rules_version))
        before = self.query("SELECT * FROM core.evaluation_runs WHERE evaluation_key=%s", (context.evaluation_key,))
        with self.assertRaises(reconciliation.EvaluationConflictError):
            self.evaluate(context)
        self.assertEqual(self.query("SELECT * FROM core.evaluation_runs WHERE evaluation_key=%s", (context.evaluation_key,)), before)
        self.assertEqual(self.canonical_snapshot(context), [[], []])
        print("T03-11 native same evaluation key with different as-of context rejected; existing attempt unchanged; no canonical publication", flush=True)

    def test_existing_canonical_result_conflict_is_rejected_not_repaired(self):
        self.seed()
        context = self.context()
        self.evaluate(context)
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute("UPDATE core.reconciliation_results SET expected_cents=12345 WHERE evaluation_key=%s AND order_id=%s", (context.evaluation_key, self.oid("f01")))
        corrupted = self.canonical_snapshot(context)
        with self.assertRaises(reconciliation.EvaluationConflictError):
            self.evaluate(context)
        self.assertEqual(self.canonical_snapshot(context), corrupted)
        self.assertEqual(self.query("SELECT status FROM core.evaluation_runs WHERE evaluation_key=%s ORDER BY status", (context.evaluation_key,)), [("FAILED",), ("SUCCEEDED",)])
        print("T03-11 native changed canonical expected value rejected on replay; no overwrite; failed attempt distinguishable", flush=True)

    def test_historical_cutoff_and_future_source_update_exclusion(self):
        self.seed()
        historical = self.context()
        self.evaluate(historical)
        baseline = self.canonical_snapshot(historical)
        later_payment = self.payment("f01", payment_id=self.oid("payment-f01"), external_charge_ref=self.oid("charge-f01"), source_version="later-conflict", amount_raw="90.00")
        self.ingest("payments", [later_payment], suffix="later-payment-version.jsonl")
        self.ingest("orders", [self.order("f08", source_version="future-source-update", source_updated_at="2026-02-05T00:00:00Z", amount_raw="130.00")], suffix="future-order-version.jsonl")
        self.assertEqual(self.evaluate(historical)["action"], "UNCHANGED")
        self.assertEqual(self.canonical_snapshot(historical), baseline)
        current = self.context()
        self.evaluate(current)
        self.assertEqual(self.result(current, "f01")[4], "UNDETERMINED")
        self.assertEqual(self.result(current, "f08"), (8000, 8000, 2000, 6000, "MATCHED"))
        print("T03-12 native historical cutoff replay unchanged after later batch; expanded cutoff sees material conflict; future-dated source update excluded at old as-of", flush=True)

    def test_repeatable_read_snapshot_survives_concurrent_owned_source_mutation(self):
        batches = self.seed()
        context = self.context()
        self.compiled[context.evaluation_key] = reconciliation.compile_model(context)
        state = {"factory_calls": 0, "mutated": False, "isolation": None}
        outer = self

        class SnapshotCursor(extensions.cursor):
            def execute(self, statement, variables=None):
                if isinstance(statement, str) and statement.startswith("-- retail-evaluation-context:") and not state["mutated"]:
                    super().execute("SHOW transaction_isolation")
                    state["isolation"] = self.fetchone()[0]
                    connection = ingestion.connect_from_env()
                    try:
                        with connection:
                            with connection.cursor() as cursor:
                                cursor.execute("UPDATE bronze.orders_source SET amount_cents=7777 WHERE batch_id=%s AND order_id=%s", (batches["orders"].batch_id, outer.oid("f03")))
                                outer.assertEqual(cursor.rowcount, 1)
                    finally:
                        connection.close()
                    state["mutated"] = True
                return super().execute(statement, variables)

        class SnapshotConnection(extensions.connection):
            def cursor(self, *args, **kwargs):
                kwargs.setdefault("cursor_factory", SnapshotCursor)
                return super().cursor(*args, **kwargs)

        def factory():
            state["factory_calls"] += 1
            if state["factory_calls"] != 2:
                return ingestion.connect_from_env()
            return psycopg2.connect(host=os.environ.get("PGHOST", "postgres"), port=os.environ.get("PGPORT", "5432"),
                dbname=os.environ.get("PGDATABASE", "retail"), user=os.environ.get("PGUSER", "retail"),
                password=os.environ["RETAIL_DB_PASSWORD"], connection_factory=SnapshotConnection)

        self.evaluate(context, connection_factory=factory)
        self.assertTrue(state["mutated"])
        self.assertEqual(state["isolation"], "repeatable read")
        self.assertEqual(self.result(context, "f03"), (4500, 0, 0, 0, "EXCEPTION"))
        self.assertEqual(self.query("SELECT amount_cents FROM bronze.orders_source WHERE batch_id=%s AND order_id=%s", (batches["orders"].batch_id, self.oid("f03"))), [(7777,)])
        print("T03-12 native work isolation=repeatable read; concurrent owned source changed to7777 after snapshot, evaluation still observed4500", flush=True)

    def test_incomplete_source_readiness_prevents_missing_payment(self):
        self.seed()
        self.ingest("refunds", b"", suffix="explicit-incomplete-scope.jsonl", complete=False)
        context = self.context()
        self.evaluate(context)
        self.assertEqual(self.result(context, "f03")[4], "UNDETERMINED")
        self.assertEqual(self.reasons(context, "f03"), [])
        self.assertEqual(self.result(context, "f01")[4], "UNDETERMINED")
        print("T03-08 native one explicitly incomplete source batch prevents MATCHED and MISSING_PAYMENT despite passed due time", flush=True)

    def test_registered_batch_is_excluded_from_committed_snapshot(self):
        self.seed()
        raw = json.dumps(self.order("registered-only"), separators=(",", ":")) + "\n"
        batch = ingestion.BatchInput("orders", "synthetic-t03/" + self.prefix + "/registered.jsonl", True, raw.encode())
        self.owned_batches.append(batch.batch_id)
        parsed = ingestion.parse_record("orders", raw)
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute("INSERT INTO bronze.input_batches(batch_id,source_name,content_digest,input_reference,row_count,status,scope_complete) VALUES (%s,'orders',%s,%s,1,'REGISTERED',true)", (batch.batch_id, batch.content_digest, batch.input_reference))
                cursor.execute("INSERT INTO bronze.orders_source(batch_id,source_row_number,source_record_id,order_id,source_version,event_at,source_updated_at,order_status,amount_raw,amount_cents,currency,payment_due_at,raw_payload) VALUES (%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (batch.batch_id,) + tuple(parsed[name] for name in ("source_record_id", "order_id", "source_version", "event_at", "source_updated_at", "order_status", "amount_raw", "amount_cents", "currency", "payment_due_at", "raw_payload")))
        self.ingest("refunds", b"", suffix="cutoff-after-registered.jsonl")
        context = self.context()
        self.evaluate(context)
        self.assertEqual(self.query("SELECT count(*) FROM core.reconciliation_results WHERE evaluation_key=%s AND order_id=%s", (context.evaluation_key, self.oid("registered-only"))), [(0,)])
        self.assertEqual(self.result(context, "f01")[4], "MATCHED")
        print("T03-12 native REGISTERED batch below committed cutoff excluded; unrelated committed F01 remains MATCHED", flush=True)

    def test_exception_and_matched_source_lineage_resolves_to_original_rows(self):
        self.seed()
        context = self.context()
        outcome = self.evaluate(context)
        references, details = self.query("SELECT evidence_refs,details FROM core.reconciliation_findings WHERE evaluation_key=%s AND order_id=%s AND reason_code='MISSING_PAYMENT'", (context.evaluation_key, self.oid("f03")))[0]
        self.assertIsInstance(references, list)
        self.assertEqual({item["source_name"] for item in references}, {"orders", "payments"})
        self.assertIsInstance(details, dict)
        for reference in references:
            row = self.query(sql.SQL("SELECT batch_id,source_row_number,source_record_id,source_version,order_id FROM {} WHERE row_id=%s").format(sql.Identifier("bronze", reference["source_name"] + "_source")), (reference["row_id"],))
            self.assertEqual(row, [(reference["batch_id"], reference["source_row_number"], reference["source_record_id"], reference["source_version"], self.oid("f03"))])
        f01 = next(row for row in outcome["results"] if row["order_id"] == self.oid("f01"))
        self.assertEqual(len(f01["evidence_refs"]), 5)
        demo = self.query("SELECT r.order_id,r.expected_cents,r.payments_cents,r.refunds_cents,r.net_cents,r.overall_status,f.reason_code FROM core.reconciliation_results r LEFT JOIN core.reconciliation_findings f USING(evaluation_key,order_id) WHERE r.evaluation_key=%s AND r.order_id=ANY(%s) ORDER BY r.order_id", (context.evaluation_key, [self.oid("f01"), self.oid("f03"), self.oid("f08")]))
        self.assertEqual(demo, [(self.oid("f01"), 10000, 10000, 0, 10000, "MATCHED", None),
                               (self.oid("f03"), 4500, 0, 0, 0, "EXCEPTION", "MISSING_PAYMENT"),
                               (self.oid("f08"), 8000, 8000, 2000, 6000, "MATCHED", None)])
        print("T03-13 native Core SQL result/finding report matches independent F01/F03/F08 rows; every Missing Payment source reference joins original Bronze occurrence; matchedF01 retains5lineage refs", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
