"""Native PostgreSQL integration evidence for M1-T02; no mocked database.

Every test owns uniquely named synthetic batches and deletes only those rows.
The suite must fail, rather than skip, when native PostgreSQL is unavailable.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
import uuid

import psycopg2
from psycopg2 import extensions, sql

import retail_ingestion as ingestion


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "synthetic"
SOURCES = ("orders", "payments", "refunds")
EXPECTED_COUNTS = {"orders": 6, "payments": 6, "refunds": 4}


def expected_identity(source, reference, content):
    """Independent implementation of the documented public identity contract."""
    digest = hashlib.sha256(content).hexdigest()
    identity_text = json.dumps(
        ["retail-jsonl-v1", source, reference, digest],
        ensure_ascii=False, separators=(",", ":"),
    )
    return digest, hashlib.sha256(identity_text.encode("utf-8")).hexdigest()


class NativePostgresTests(unittest.TestCase):
    def setUp(self):
        self.prefix = "synthetic-native-t02/" + uuid.uuid4().hex
        self.owned_ids = []
        self.writer = ingestion.connect_from_env()
        self.reader = ingestion.connect_from_env()
        self.reader.autocommit = True

    def tearDown(self):
        # Roll back a failed test transaction before scoped cleanup. Never reset
        # a database/schema, drop objects, or touch another batch's records.
        try:
            self.writer.rollback()
            if self.owned_ids:
                with self.writer:
                    with self.writer.cursor() as cursor:
                        for source in SOURCES:
                            cursor.execute(
                                sql.SQL("DELETE FROM {} WHERE batch_id=ANY(%s)").format(
                                    sql.Identifier("bronze", source + "_source")
                                ), (self.owned_ids,),
                            )
                        cursor.execute(
                            "DELETE FROM bronze.input_batches WHERE batch_id=ANY(%s)",
                            (self.owned_ids,),
                        )
                with self.reader.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM bronze.input_batches WHERE batch_id=ANY(%s)",
                        (self.owned_ids,),
                    )
                    self.assertEqual(cursor.fetchone()[0], 0, "Owned synthetic cleanup failed")
        finally:
            self.writer.close()
            self.reader.close()

    def batch(self, source="orders", content=None, suffix=None, complete=True):
        if content is None:
            content = (FIXTURES / (source + ".jsonl")).read_bytes()
        reference = self.prefix + "/" + (suffix or source + ".jsonl")
        _, batch_id = expected_identity(source, reference, content)
        if batch_id not in self.owned_ids:
            self.owned_ids.append(batch_id)
        return ingestion.BatchInput(source, reference, complete, content)

    def query(self, statement, parameters=()):
        with self.reader.cursor() as cursor:
            cursor.execute(statement, parameters)
            return cursor.fetchall()

    def snapshot(self):
        result = [self.query(
            "SELECT * FROM bronze.input_batches WHERE batch_id=ANY(%s) ORDER BY batch_id",
            (self.owned_ids,),
        )]
        for source in SOURCES:
            result.append(self.query(
                sql.SQL("SELECT * FROM {} WHERE batch_id=ANY(%s) ORDER BY batch_id,source_row_number").format(
                    sql.Identifier("bronze", source + "_source")
                ), (self.owned_ids,),
            ))
        return result

    def assert_no_persisted_batch(self, batch):
        self.assertEqual(self.query(
            "SELECT count(*) FROM bronze.input_batches WHERE batch_id=%s", (batch.batch_id,),
        ), [(0,)])
        for source in SOURCES:
            self.assertEqual(self.query(
                sql.SQL("SELECT count(*) FROM {} WHERE batch_id=%s").format(
                    sql.Identifier("bronze", source + "_source")
                ), (batch.batch_id,),
            ), [(0,)])

    def test_three_sources_counts_lineage_and_metadata(self):
        observed = {}
        sequences = []
        for source, expected_count in EXPECTED_COUNTS.items():
            batch = self.batch(source)
            digest, batch_id = expected_identity(source, batch.input_reference, batch.content)
            result = ingestion.ingest_batch(self.writer, batch)
            self.assertEqual(result["action"], "COMMITTED")
            self.assertEqual(result["row_count"], expected_count)
            self.assertEqual(result["batch_id"], batch_id)
            self.assertEqual(result["content_digest"], digest)
            metadata = self.query(
                "SELECT source_name,content_digest,input_reference,row_count,scope_complete,"
                "status,committed_at,batch_sequence FROM bronze.input_batches WHERE batch_id=%s",
                (batch_id,),
            )[0]
            self.assertEqual(metadata[:6], (
                source, digest, batch.input_reference, expected_count, True, "COMMITTED",
            ))
            self.assertIsNotNone(metadata[6])
            sequences.append(metadata[7])
            rows = self.query(
                sql.SQL("SELECT source_row_number,raw_payload FROM {} WHERE batch_id=%s ORDER BY source_row_number").format(
                    sql.Identifier("bronze", source + "_source")
                ), (batch_id,),
            )
            observed[source] = len(rows)
            self.assertEqual([row[0] for row in rows], list(range(1, expected_count + 1)))
            self.assertEqual(b"".join(row[1].encode("utf-8") for row in rows), batch.content)
            print(f"T02-04 native {source}: digest={digest}, batch={batch_id}, lineage=1..{expected_count}", flush=True)
        self.assertEqual(observed, EXPECTED_COUNTS)
        self.assertEqual(len(set(sequences)), 3)
        print(f"T02-03 native expected={EXPECTED_COUNTS}, observed={observed}, committed_batches=3", flush=True)

    def test_handwritten_money_invalid_fields_duplicates_and_orphans(self):
        batches = {source: self.batch(source) for source in SOURCES}
        for batch in batches.values():
            ingestion.ingest_batch(self.writer, batch)
        # This ledger is authored independently from the parser and fixture JSON.
        # It asserts stored source occurrences, never reconciliation outcomes.
        expected_money = {
            "orders": [(1, "100.00", 10000), (2, "45.00", 4500), (3, "80.00", 8000),
                       (4, "100.00", 10000), (5, "12.345", None), (6, None, None)],
            "payments": [(1, "100.00", 10000), (2, "80.00", 8000), (3, "45.00", 4500),
                         (4, "100.00", 10000), (5, "7.00", 700), (6, "NaN", None)],
            "refunds": [(1, "20.00", 2000), (2, "1.00", 100), (3, "1.2", None), (4, None, None)],
        }
        for source in SOURCES:
            actual = self.query(
                sql.SQL("SELECT source_row_number,amount_raw,amount_cents FROM {} "
                        "WHERE batch_id=%s ORDER BY source_row_number").format(
                    sql.Identifier("bronze", source + "_source")
                ), (batches[source].batch_id,),
            )
            self.assertEqual(actual, expected_money[source])
            print(f"T02-05 native {source}: expected={expected_money[source]}, observed={actual}", flush=True)
        self.assertEqual(self.query(
            "SELECT order_status,event_at,source_updated_at,payment_due_at FROM bronze.orders_source "
            "WHERE batch_id=%s AND source_row_number=5", (batches["orders"].batch_id,),
        ), [("UNRECOGNIZED", None, None, None)])
        self.assertEqual(self.query(
            "SELECT payment_status,source_version,event_at,source_updated_at,financial_effective_at "
            "FROM bronze.payments_source WHERE batch_id=%s AND source_row_number=6",
            (batches["payments"].batch_id,),
        ), [("MYSTERY", None, None, None, None)])
        self.assertEqual(self.query(
            "SELECT refund_status,event_at,financial_effective_at,payment_id FROM bronze.refunds_source "
            "WHERE batch_id=%s AND source_row_number=3", (batches["refunds"].batch_id,),
        ), [("MYSTERY", None, None, None)])
        self.assertEqual(self.query(
            "SELECT refund_id,amount_cents,raw_payload FROM bronze.refunds_source "
            "WHERE batch_id=%s AND source_row_number=4", (batches["refunds"].batch_id,),
        ), [(None, None, "\n")])
        self.assertEqual(self.query(
            "SELECT event_at,source_updated_at FROM bronze.orders_source WHERE batch_id=%s AND source_row_number=1",
            (batches["orders"].batch_id,),
        ), [(datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc))])
        self.assertEqual(self.query(
            "SELECT source_version,amount_cents FROM bronze.orders_source "
            "WHERE batch_id=%s AND order_id='syn-order-paid' ORDER BY source_row_number",
            (batches["orders"].batch_id,),
        ), [("0001", 10000), ("0002", 10000)])
        self.assertEqual(self.query(
            "SELECT source_version,external_charge_ref,amount_cents FROM bronze.payments_source "
            "WHERE batch_id=%s AND payment_id='syn-payment-paid' ORDER BY source_row_number",
            (batches["payments"].batch_id,),
        ), [("0001", "syn-charge-paid", 10000), ("0002", "syn-charge-paid", 10000)])
        self.assertEqual(self.query(
            "SELECT order_id,payment_id FROM bronze.refunds_source WHERE batch_id=%s AND source_row_number=2",
            (batches["refunds"].batch_id,),
        ), [("syn-order-absent", "syn-payment-absent")])
        self.assertEqual(self.query(
            "SELECT order_id FROM bronze.payments_source WHERE batch_id=%s AND source_row_number=5",
            (batches["payments"].batch_id,),
        ), [("syn-order-absent",)])
        self.assertEqual(self.query(
            "SELECT count(*) FROM bronze.orders_source WHERE batch_id=%s AND order_id='syn-order-absent'",
            (batches["orders"].batch_id,),
        ), [(0,)])
        print("T02-06/07 native invalid monetary/status/timestamps, duplicate IDs/versions/charge refs and orphan references retained", flush=True)

    def test_native_bigint_edges_without_float_or_rounding(self):
        content = (
            b'{"synthetic":true,"amount_raw":"92233720368547758.07","currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":"-92233720368547758.08","currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":"92233720368547758.08","currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":"0.10","currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":0.10,"currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":"1.001","currency":"USD"}\n'
            b'{"synthetic":true,"amount_raw":"1.00","currency":"EUR"}\n'
        )
        batch = self.batch("payments", content=content, suffix="money-boundaries.jsonl")
        ingestion.ingest_batch(self.writer, batch)
        actual = self.query(
            "SELECT amount_cents,pg_typeof(amount_cents)::text FROM bronze.payments_source "
            "WHERE batch_id=%s ORDER BY source_row_number", (batch.batch_id,),
        )
        expected = [(9223372036854775807, "bigint"), (-9223372036854775808, "bigint"),
                    (None, "bigint"), (10, "bigint"), (None, "bigint"), (None, "bigint"), (None, "bigint")]
        self.assertEqual(actual, expected)
        print(f"T02-05 native BIGINT edges and invalid precision/type/currency expected={expected}, observed={actual}", flush=True)

    def test_two_repeated_ingestions_preserve_every_persisted_column(self):
        batches = [self.batch(source) for source in SOURCES]
        for batch in batches:
            ingestion.ingest_batch(self.writer, batch)
        baseline = self.snapshot()
        # A parser trap proves replay does not recompute committed business
        # values, even if recomputing would coincidentally yield equal values.
        with patch.object(ingestion, "parse_record", side_effect=AssertionError("Replay must not reparse committed records")):
            for attempt in (1, 2):
                actions = [ingestion.ingest_batch(self.writer, batch)["action"] for batch in batches]
                self.assertEqual(actions, ["UNCHANGED", "UNCHANGED", "UNCHANGED"])
                self.assertEqual(self.snapshot(), baseline)
                print(f"T02-08 native replay {attempt}: actions={actions}; all IDs, values, timestamps and sequences unchanged; parser not invoked", flush=True)

    def test_real_mid_batch_sql_failure_rolls_back_metadata_and_rows(self):
        content = (
            b'{"synthetic":true,"order_id":"synthetic-rollback-1","amount_raw":"1.00","currency":"USD"}\n'
            b'{"synthetic":true,"order_id":"synthetic-rollback-2","amount_raw":"2.00","currency":"USD"}\n'
        )
        batch = self.batch(content=content, suffix="rollback.jsonl")
        original_parser = ingestion.parse_record
        calls = 0
        observed_before_failure = None

        def fail_with_native_sql(source, raw_line):
            nonlocal calls, observed_before_failure
            parsed = original_parser(source, raw_line)
            calls += 1
            if calls == 2:
                with self.writer.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM bronze.orders_source WHERE batch_id=%s", (batch.batch_id,))
                    observed_before_failure = cursor.fetchone()[0]
                    cursor.execute("SELECT 1 / 0")
            return parsed

        with patch.object(ingestion, "parse_record", side_effect=fail_with_native_sql):
            with self.assertRaises(psycopg2.errors.DivisionByZero) as rejected:
                ingestion.ingest_batch(self.writer, batch)
        self.assertEqual(rejected.exception.pgcode, "22012")
        self.assertEqual(observed_before_failure, 1, "Failure must occur after one real raw insert")
        self.assertEqual(self.writer.get_transaction_status(), extensions.TRANSACTION_STATUS_IDLE)
        self.assert_no_persisted_batch(batch)
        print("T02-09 native expected/observed SQLSTATE 22012 after 1 raw insert; persisted batches=0, rows=0", flush=True)

    def test_scope_conflict_and_tampered_metadata_are_not_rewritten(self):
        batch = self.batch()
        ingestion.ingest_batch(self.writer, batch)
        baseline = self.snapshot()
        changed_scope = ingestion.BatchInput(
            batch.source_name, batch.input_reference, False, batch.content,
        )
        self.assertEqual(changed_scope.batch_id, batch.batch_id)
        with self.assertRaises(ingestion.BatchConflictError):
            ingestion.ingest_batch(self.writer, changed_scope)
        self.assertEqual(self.snapshot(), baseline)
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze.input_batches SET content_digest='synthetic-deliberate-corruption' WHERE batch_id=%s",
                    (batch.batch_id,),
                )
        corrupted = self.snapshot()
        with self.assertRaises(ingestion.BatchConflictError):
            ingestion.ingest_batch(self.writer, batch)
        self.assertEqual(self.snapshot(), corrupted)
        print("T02-10 native scope conflict and corrupted digest rejected; no existing metadata or rows rewritten", flush=True)

    def test_tampered_raw_lineage_and_wrong_source_children_are_rejected(self):
        batch = self.batch()
        ingestion.ingest_batch(self.writer, batch)
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze.orders_source SET raw_payload='synthetic deliberately altered payload' "
                    "WHERE batch_id=%s AND source_row_number=1", (batch.batch_id,),
                )
        corrupted = self.snapshot()
        with self.assertRaises(ingestion.BatchConflictError):
            ingestion.ingest_batch(self.writer, batch)
        self.assertEqual(self.snapshot(), corrupted)

        other = self.batch(suffix="wrong-source.jsonl")
        ingestion.ingest_batch(self.writer, other)
        with self.writer:
            with self.writer.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO bronze.payments_source(batch_id,source_row_number,raw_payload) VALUES (%s,1,%s)",
                    (other.batch_id, 'synthetic deliberately misplaced child'),
                )
        wrong_source = self.snapshot()
        with self.assertRaises(ingestion.BatchConflictError):
            ingestion.ingest_batch(self.writer, other)
        self.assertEqual(self.snapshot(), wrong_source)
        print("T02-10 native raw-payload and wrong-source-child conflicts rejected without repair or overwrite", flush=True)

    def test_empty_readiness_and_exact_crlf_blank_unterminated_lines(self):
        empty = self.batch("refunds", content=b"", suffix="empty.jsonl")
        result = ingestion.ingest_batch(self.writer, empty)
        self.assertEqual((result["action"], result["row_count"], result["scope_complete"]), ("COMMITTED", 0, True))
        self.assertEqual(ingestion.ingest_batch(self.writer, empty)["action"], "UNCHANGED")
        content = '{"synthetic":true,"order_id":"synthetic-caf\u00e9"}\r\n\r\n{malformed synthetic'.encode("utf-8")
        batch = self.batch(content=content, suffix="line-endings.jsonl", complete=False)
        self.assertEqual(ingestion.ingest_batch(self.writer, batch)["row_count"], 3)
        self.assertEqual(self.query(
            "SELECT source_row_number,raw_payload FROM bronze.orders_source WHERE batch_id=%s ORDER BY source_row_number",
            (batch.batch_id,),
        ), [
            (1, '{"synthetic":true,"order_id":"synthetic-caf\u00e9"}\r\n'),
            (2, "\r\n"),
            (3, "{malformed synthetic"),
        ])
        print("T02-04/06 native zero-row readiness and exact UTF-8, CRLF, blank, malformed, final unterminated records retained", flush=True)

    def test_unrepresentable_text_fails_without_partial_batch(self):
        for name, content in (
            ("invalid-utf8.jsonl", b'{"synthetic":true}\n\xff\n'),
            ("actual-nul.jsonl", b'{"synthetic":true}\n\x00\n'),
        ):
            with self.subTest(name=name):
                batch = self.batch(content=content, suffix=name)
                with self.assertRaises(ingestion.IngestionError):
                    ingestion.ingest_batch(self.writer, batch)
                self.assert_no_persisted_batch(batch)
        print("T02-09 native unrepresentable UTF-8/NUL inputs rejected; no partial batches/rows persisted", flush=True)

    def test_concurrent_identical_ingestion_commits_once(self):
        batch = self.batch()
        ready = threading.Barrier(2)

        def ingest_once():
            connection = ingestion.connect_from_env()
            try:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout='15s'")
                connection.autocommit = False
                ready.wait(timeout=15)
                return ingestion.ingest_batch(connection, batch)["action"]
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [pool.submit(ingest_once) for _ in range(2)]
            actions = sorted(result.result(timeout=30) for result in results)
        self.assertEqual(actions, ["COMMITTED", "UNCHANGED"])
        self.assertEqual(self.query(
            "SELECT count(*) FROM bronze.input_batches WHERE batch_id=%s", (batch.batch_id,),
        ), [(1,)])
        self.assertEqual(self.query(
            "SELECT count(*) FROM bronze.orders_source WHERE batch_id=%s", (batch.batch_id,),
        ), [(6,)])
        print(f"T02-08 native concurrent identical batch actions={actions}; batches=1, rows=6", flush=True)

    def test_eight_table_foundation_and_core_data_are_unchanged(self):
        expected = [
            ("bronze", "input_batches"), ("bronze", "orders_source"),
            ("bronze", "payments_source"), ("bronze", "refunds_source"),
            ("core", "dq_findings"), ("core", "evaluation_runs"),
            ("core", "reconciliation_findings"), ("core", "reconciliation_results"),
        ]
        self.assertEqual(self.query(
            "SELECT schemaname,tablename FROM pg_tables "
            "WHERE schemaname !~ '^pg_' AND schemaname <> 'information_schema' ORDER BY 1,2"
        ), expected)
        core_names = ("dq_findings", "evaluation_runs", "reconciliation_findings", "reconciliation_results")
        before = [self.query(sql.SQL("SELECT * FROM {} ORDER BY 1,2").format(sql.Identifier("core", name))) for name in core_names]
        ingestion.ingest_batch(self.writer, self.batch())
        after = [self.query(sql.SQL("SELECT * FROM {} ORDER BY 1,2").format(sql.Identifier("core", name))) for name in core_names]
        self.assertEqual(after, before)
        print("T02-11 native exactly eight original Retail tables; ingestion left all core records unchanged", flush=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
