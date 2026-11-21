"""Independent M1-T02 parsing/manifest fixtures; no reconciliation classifications."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from retail_ingestion import (
    BatchInput,
    load_manifest,
    parse_amount,
    parse_record,
    parse_timestamp,
)


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "synthetic"


class ExactMoneyTests(unittest.TestCase):
    def test_t02_05_exact_signed_scale_two_and_bigint_boundaries(self):
        expected = {
            "0.00": 0,
            "0001.23": 123,
            "+1.23": 123,
            "-1.23": -123,
            "92233720368547758.07": 9223372036854775807,
            "-92233720368547758.08": -9223372036854775808,
        }
        for raw, cents in expected.items():
            with self.subTest(raw=raw):
                observed = parse_amount(raw, "USD")
                self.assertIs(type(observed), int)
                self.assertEqual(observed, cents)

    def test_t02_05_invalid_representation_is_not_rounded_or_coerced(self):
        invalid = (
            "1", "1.2", "1.234", "1.2300", "1e2", "NaN", "Infinity",
            " 1.00", "1.00 ", "1.00\n", ".01", "1,000.00", "\u0661.00",
            "92233720368547758.08", "-92233720368547758.09", "", None,
            123, 1.23, True, {}, [],
        )
        for raw in invalid:
            with self.subTest(raw=raw):
                self.assertIsNone(parse_amount(raw, "USD"))
        for currency in ("EUR", "usd", " USD", None, 1):
            with self.subTest(currency=currency):
                self.assertIsNone(parse_amount("1.00", currency))


class TimestampTests(unittest.TestCase):
    def test_t02_06_explicit_offsets_normalize_to_utc(self):
        expected = datetime(2026, 1, 1, 0, 30, 0, 123456, tzinfo=timezone.utc)
        for text in (
            "2026-01-01T00:30:00.123456Z",
            "2026-01-01T07:30:00.123456+07:00",
            "2025-12-31T19:30:00.123456-05:00",
        ):
            with self.subTest(text=text):
                self.assertEqual(parse_timestamp(text), expected)

    def test_t02_06_invalid_naive_or_excess_precision_stays_null(self):
        for text in (
            "2026-01-01", "2026-01-01T00:00:00", "not-a-date",
            "2026-02-30T00:00:00Z", "2026-01-01T00:00:00.1234567Z",
            "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+00:60",
            None, False, 123, {}, [],
        ):
            with self.subTest(text=text):
                self.assertIsNone(parse_timestamp(text))


class RawEvidenceTests(unittest.TestCase):
    def test_t02_06_original_line_and_source_text_are_unchanged(self):
        line = (' {"order_id":"syn-order-é","source_version":"0007",'
                '"amount_raw":"0001.23","currency":"USD",'
                '"order_status":"UNRECOGNIZED","event_at":"not-a-date",'
                '"extra":{"kept":true}}\r\n')
        actual = parse_record("orders", line)
        self.assertEqual(actual["raw_payload"], line)
        self.assertEqual(actual["order_id"], "syn-order-é")
        self.assertEqual(actual["source_version"], "0007")
        self.assertEqual(actual["amount_raw"], "0001.23")
        self.assertEqual(actual["amount_cents"], 123)
        self.assertEqual(actual["order_status"], "UNRECOGNIZED")
        self.assertIsNone(actual["event_at"])

    def test_t02_06_invalid_json_record_has_raw_line_and_null_parsed_fields(self):
        for line in ("{bad-json\n", "\n", "[]\n", "null\n", "123\n",
                     '{"order_id":"first","order_id":"second"}\n',
                     '{"order_id":"syn-order","extra":NaN}\n'):
            with self.subTest(line=line):
                actual = parse_record("orders", line)
                self.assertEqual(actual["raw_payload"], line)
                self.assertGreater(len(actual), 1)
                self.assertTrue(all(value is None for key, value in actual.items()
                                    if key != "raw_payload"))

    def test_t02_06_extreme_json_numeric_exponent_retains_raw_without_parse_failure(self):
        for exponent in ("999999999999999999999999999999999999",
                         "-999999999999999999999999999999999999"):
            line = '{"order_id":"syn-extreme-exponent","extra":1e' + exponent + '}\n'
            with self.subTest(exponent=exponent):
                actual = parse_record("orders", line)
                self.assertEqual(actual["raw_payload"], line)
                self.assertTrue(all(value is None for key, value in actual.items()
                                    if key != "raw_payload"))

    def test_t02_06_nonstring_fields_not_silently_coerced(self):
        line = '{"order_id":5,"source_version":3,"amount_raw":1.23,"currency":"USD","order_status":false,"event_at":0}\n'
        actual = parse_record("orders", line)
        for column in ("order_id", "source_version", "amount_raw", "amount_cents",
                       "order_status", "event_at"):
            with self.subTest(column=column):
                self.assertIsNone(actual[column])
        self.assertEqual(actual["raw_payload"], line)

    def test_t02_06_escaped_unrepresentable_text_keeps_raw_and_null_parsed_field(self):
        line = '{"order_id":"syn-\\u0000-order","source_version":"\\ud800"}\n'
        actual = parse_record("orders", line)
        self.assertEqual(actual["raw_payload"], line)
        self.assertIsNone(actual["order_id"])
        self.assertIsNone(actual["source_version"])

    def test_t02_07_orphan_ids_and_optional_refund_payment_are_retained(self):
        raw = '{"refund_id":"syn-refund","order_id":"syn-absent","amount_raw":"1.00","currency":"USD"}\n'
        actual = parse_record("refunds", raw)
        self.assertEqual(actual["refund_id"], "syn-refund")
        self.assertEqual(actual["order_id"], "syn-absent")
        self.assertIsNone(actual["payment_id"])
        self.assertEqual(actual["amount_cents"], 100)


class IdentityAndManifestTests(unittest.TestCase):
    def test_t02_04_only_lf_splits_records_and_original_terminators_survive(self):
        expected = ('{"synthetic":true,"note":"café"}\r\n', '\n', 'synthetic-bad\vline\r\u0085tail')
        content = ''.join(expected).encode("utf-8")
        spec = BatchInput("orders", "synthetic/lines.jsonl", True, content)
        self.assertEqual(spec.raw_lines, expected)
        self.assertEqual(''.join(spec.raw_lines).encode("utf-8"), content)
        self.assertEqual(BatchInput("orders", "empty.jsonl", False, b"").raw_lines, ())
        self.assertEqual(BatchInput("orders", "blank.jsonl", True, b"\n").raw_lines, ("\n",))
        self.assertEqual(BatchInput("orders", "one.jsonl", True, b"{}\n").raw_lines, ("{}\n",))

    def test_t02_06_raw_bytes_unrepresentable_as_postgresql_text_fail_without_dropping(self):
        for content in (b"\xff\n", b"synthetic\x00line\n"):
            with self.subTest(content=content), self.assertRaises(ValueError):
                BatchInput("orders", "synthetic/bad-text.jsonl", True, content).raw_lines

    def test_t02_04_identity_uses_source_reference_and_exact_file_bytes(self):
        content = b'{"synthetic":true}\r\n\n'
        spec = BatchInput("orders", "synthetic/orders.jsonl", True, content)
        digest = hashlib.sha256(content).hexdigest()
        independent_identity_text = json.dumps(
            ["retail-jsonl-v1", "orders", "synthetic/orders.jsonl", digest],
            ensure_ascii=False, separators=(",", ":"),
        )
        expected_identity = hashlib.sha256(independent_identity_text.encode("utf-8")).hexdigest()
        self.assertEqual(spec.content_digest, digest)
        self.assertEqual(spec.batch_id, expected_identity)
        self.assertEqual(spec.batch_id, BatchInput("orders", "synthetic/orders.jsonl", False, content).batch_id)
        for changed in (
            BatchInput("payments", "synthetic/orders.jsonl", True, content),
            BatchInput("orders", "synthetic/other.jsonl", True, content),
            BatchInput("orders", "synthetic/orders.jsonl", True, content.replace(b"\r\n", b"\n")),
        ):
            self.assertNotEqual(spec.batch_id, changed.batch_id)

    def test_t02_02_manifest_explicit_three_source_scope_and_independent_counts(self):
        specs = load_manifest(FIXTURES / "manifest.json")
        self.assertEqual([spec.source_name for spec in specs], ["orders", "payments", "refunds"])
        self.assertEqual([spec.input_reference for spec in specs], ["orders.jsonl", "payments.jsonl", "refunds.jsonl"])
        self.assertEqual([spec.scope_complete for spec in specs], [True, True, True])
        expected_counts = {"orders": 6, "payments": 6, "refunds": 4}
        for spec in specs:
            with self.subTest(source=spec.source_name):
                self.assertEqual(len(spec.content.splitlines(keepends=True)), expected_counts[spec.source_name])
                self.assertEqual(spec.content, (FIXTURES / spec.input_reference).read_bytes())

    def test_t02_02_missing_or_nonstrict_scope_flag_is_rejected(self):
        for completeness in ("missing", None, "true", 1):
            with self.subTest(scope_complete=completeness), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                manifest = self._write_empty_inputs(directory)
                if completeness == "missing":
                    del manifest["inputs"][0]["scope_complete"]
                else:
                    manifest["inputs"][0]["scope_complete"] = completeness
                (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_manifest(directory / "manifest.json")

    def test_t02_02_manifest_requires_explicit_synthetic_label(self):
        for synthetic in (None, False, "true", 1):
            with self.subTest(synthetic=synthetic), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                manifest = self._write_empty_inputs(directory)
                manifest["synthetic"] = synthetic
                (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_manifest(directory / "manifest.json")

    def test_t02_02_manifest_rejects_missing_duplicate_or_unknown_source(self):
        for replacement in ("missing", "duplicate", "unknown"):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                manifest = self._write_empty_inputs(directory)
                if replacement == "missing":
                    manifest["inputs"].pop()
                elif replacement == "duplicate":
                    manifest["inputs"][2]["source"] = "orders"
                else:
                    manifest["inputs"][2]["source"] = "other"
                (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_manifest(directory / "manifest.json")

    def test_t02_02_manifest_rejects_unsafe_input_reference(self):
        for reference in ("../escape.jsonl", "/absolute.jsonl", "a/../../escape.jsonl", "C:\\escape.jsonl"):
            with self.subTest(reference=reference), tempfile.TemporaryDirectory() as folder:
                directory = Path(folder)
                manifest = self._write_empty_inputs(directory)
                manifest["inputs"][0]["path"] = reference
                (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_manifest(directory / "manifest.json")

    def test_t02_02_distinct_sources_cannot_reuse_one_resolved_input_file(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            manifest = self._write_empty_inputs(directory)
            manifest["inputs"][1]["path"] = "orders.jsonl"
            manifest["inputs"][2]["path"] = "orders.jsonl"
            (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_manifest(directory / "manifest.json")

    def test_t02_02_empty_sources_require_explicit_scope_and_keep_zero_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            manifest = self._write_empty_inputs(directory)
            manifest["inputs"][1]["scope_complete"] = False
            (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            specs = load_manifest(directory / "manifest.json")
            self.assertEqual([spec.content for spec in specs], [b"", b"", b""])
            self.assertEqual([spec.scope_complete for spec in specs], [True, False, True])

    @staticmethod
    def _write_empty_inputs(directory):
        names = ("orders", "payments", "refunds")
        for name in names:
            (directory / (name + ".jsonl")).write_bytes(b"")
        return {"synthetic": True, "inputs": [
            {"source": name, "path": name + ".jsonl", "scope_complete": True}
            for name in names
        ]}


class SyntheticFixtureLedgerTests(unittest.TestCase):
    def test_t02_02_fixture_ledger_is_handwritten_ingestion_evidence(self):
        orders = (FIXTURES / "orders.jsonl").read_bytes().splitlines(keepends=True)
        payments = (FIXTURES / "payments.jsonl").read_bytes().splitlines(keepends=True)
        refunds = (FIXTURES / "refunds.jsonl").read_bytes().splitlines(keepends=True)
        self.assertEqual([len(orders), len(payments), len(refunds)], [6, 6, 4])
        self.assertEqual([parse_record("orders", line.decode())["amount_cents"] for line in orders],
                         [10000, 4500, 8000, 10000, None, None])
        self.assertEqual([parse_record("payments", line.decode())["amount_cents"] for line in payments],
                         [10000, 8000, 4500, 10000, 700, None])
        self.assertEqual([parse_record("refunds", line.decode())["amount_cents"] for line in refunds],
                         [2000, 100, None, None])
        for rows in (orders[:5], payments, refunds[:3]):
            for line in rows:
                self.assertIs(json.loads(line)["synthetic"], True)
        self.assertEqual(refunds[3], b"\n")
        self.assertEqual(json.loads(payments[2])["payment_status"], "PENDING")
        self.assertEqual(json.loads(payments[4])["order_id"], "syn-order-absent")
        self.assertEqual(json.loads(refunds[1])["payment_id"], "syn-payment-absent")
        self.assertEqual(json.loads(orders[0])["order_id"], json.loads(orders[3])["order_id"])
        self.assertEqual(json.loads(payments[0])["payment_id"], json.loads(payments[3])["payment_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
