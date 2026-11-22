"""Independent context and fixture expectations for the bounded M1-T03 slice."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import unittest

from retail_ingestion import load_manifest
from retail_reconciliation import Context


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "reconciliation"


class ContextTests(unittest.TestCase):
    def test_context_key_matches_independent_canonical_tuple(self):
        context = Context(123, "2026-02-03T00:00:00Z", "m1-thin-v1")
        canonical = '["retail-evaluation-v1",123,"2026-02-03T00:00:00.000000Z","m1-thin-v1"]'
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        self.assertEqual(context.evaluation_key, expected)
        self.assertEqual(context.canonical_as_of, "2026-02-03T00:00:00.000000Z")

    def test_equivalent_offsets_and_datetimes_have_one_key(self):
        contexts = [
            Context(123, "2026-02-03T00:00:00Z"),
            Context(123, "2026-02-03T07:00:00+07:00"),
            Context(123, datetime(2026, 2, 3, tzinfo=timezone.utc)),
        ]
        self.assertEqual(len({item.evaluation_key for item in contexts}), 1)
        self.assertEqual(contexts[0].evaluation_as_of, datetime(2026, 2, 3, tzinfo=timezone.utc))

    def test_cutoff_or_asof_changes_change_key(self):
        baseline = Context(123, "2026-02-03T00:00:00Z")
        self.assertNotEqual(baseline.evaluation_key, Context(124, "2026-02-03T00:00:00Z").evaluation_key)
        self.assertNotEqual(baseline.evaluation_key, Context(123, "2026-02-03T00:00:00.000001Z").evaluation_key)

    def test_invalid_cutoffs_are_not_coerced(self):
        for value in (True, False, -1, 9223372036854775808, 1.5, "123", None):
            with self.subTest(cutoff=value), self.assertRaises((ValueError, TypeError)):
                Context(value, "2026-02-03T00:00:00Z")

    def test_naive_or_invalid_evaluation_timestamps_are_rejected(self):
        for value in ("2026-02-03", "2026-02-03T00:00:00", "not-time", "2026-02-30T00:00:00Z",
                      "2026-02-03T00:00:00.1234567Z", "2026-02-03T00:00:00+00:60",
                      datetime(2026, 2, 3), None, False):
            with self.subTest(as_of=value), self.assertRaises((ValueError, TypeError)):
                Context(123, value)

    def test_unsupported_rules_are_rejected(self):
        for value in ("", "m2", "m1-thin-v2", None, 1):
            with self.subTest(rules=value), self.assertRaises((ValueError, TypeError)):
                Context(123, "2026-02-03T00:00:00Z", value)


class ScenarioFixtureTests(unittest.TestCase):
    def test_independent_three_source_fixture_contract(self):
        batches = load_manifest(FIXTURES / "manifest.json")
        self.assertEqual([batch.source_name for batch in batches], ["orders", "payments", "refunds"])
        self.assertEqual([len(batch.raw_lines) for batch in batches], [4, 5, 1])
        self.assertTrue(all(batch.scope_complete for batch in batches))
        rows = {batch.source_name: [json.loads(line) for line in batch.raw_lines] for batch in batches}
        self.assertTrue(all(row["synthetic"] is True for source in rows.values() for row in source))
        self.assertEqual({row["order_id"] for row in rows["orders"]}, {"syn-t03-f01", "syn-t03-f03", "syn-t03-f08"})
        self.assertEqual([row["amount_raw"] for row in rows["orders"]], ["100.00", "100.00", "45.00", "80.00"])
        self.assertEqual([row["amount_raw"] for row in rows["payments"]], ["100.00", "100.00", "100.00", "45.00", "80.00"])
        self.assertEqual(rows["refunds"][0]["amount_raw"], "20.00")
        self.assertEqual(rows["payments"][3]["payment_status"], "PENDING")
        self.assertEqual({row["payment_due_at"] for row in rows["orders"]}, {"2026-02-02T00:00:00Z"})
        self.assertEqual(len({row["external_charge_ref"] for row in rows["payments"][:3]}), 1)
        self.assertEqual(len({row["payment_id"] for row in rows["payments"][:3]}), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
