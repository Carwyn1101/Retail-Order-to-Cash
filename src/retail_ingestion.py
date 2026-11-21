"""Bounded synthetic JSONL ingestion into the existing Retail Bronze tables.

Each LF-delimited record is retained verbatim, including its terminator. This
module stores occurrences; it does not select effective source versions or
calculate payment/refund/reconciliation business state.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, DecimalException
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Any

import psycopg2
from psycopg2 import extensions, sql


SOURCE_COLUMNS = {
    "orders": (
        "source_record_id", "order_id", "source_version", "event_at",
        "source_updated_at", "order_status", "amount_raw", "amount_cents",
        "currency", "payment_due_at",
    ),
    "payments": (
        "source_record_id", "payment_id", "order_id", "external_charge_ref",
        "source_version", "event_at", "source_updated_at", "financial_effective_at",
        "payment_status", "amount_raw", "amount_cents", "currency",
    ),
    "refunds": (
        "source_record_id", "refund_id", "order_id", "payment_id", "source_version",
        "event_at", "source_updated_at", "financial_effective_at", "refund_status",
        "amount_raw", "amount_cents", "currency",
    ),
}
TIME_COLUMNS = frozenset({
    "event_at", "source_updated_at", "financial_effective_at", "payment_due_at",
})
AMOUNT_PATTERN = re.compile(r"[+-]?[0-9]+\.[0-9]{2}\Z")
TIME_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)


class IngestionError(ValueError):
    """Fatal input or persisted-batch conflict; no automatic repair is attempted."""


class BatchConflictError(IngestionError):
    """Existing batch identity cannot safely be reused by this workflow."""


def _text(value: Any) -> str | None:
    """Only source strings represent text fields; do not invent coercions."""
    if not isinstance(value, str) or "\x00" in value:
        return None
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        return None
    return value


def parse_amount(raw: Any, currency: Any) -> int | None:
    """Accept USD ASCII fixed scale-2 text, within signed PostgreSQL BIGINT.

    No rounding, whitespace normalization or floating-point conversion occurs.
    Signs and leading zeroes are textual representations, not business validity
    rules; negative source evidence is retained for later business validation.
    """
    if currency != "USD" or not isinstance(raw, str) or not AMOUNT_PATTERN.fullmatch(raw):
        return None
    negative = raw.startswith("-")
    digits = raw.lstrip("+-").replace(".", "").lstrip("0") or "0"
    # Check decimal length before int(), including Python's large-string limit.
    limit = "9223372036854775808" if negative else "9223372036854775807"
    if len(digits) > len(limit) or (len(digits) == len(limit) and digits > limit):
        return None
    cents = int(digits)
    return -cents if negative else cents


def parse_timestamp(value: Any) -> datetime | None:
    """Normalize explicit-offset ISO timestamps to UTC; reject implicit zones."""
    if not isinstance(value, str) or not TIME_PATTERN.fullmatch(value):
        return None
    # datetime.fromisoformat can normalize invalid offsets such as +00:60.
    # Reject them instead of silently changing the source timestamp.
    if not value.endswith("Z"):
        offset = value[-6:]
        if int(offset[1:3]) > 23 or int(offset[4:6]) > 59:
            return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ValueError("Nonstandard JSON numeric constant")


def _json_object(text: str) -> dict[str, Any]:
    value = json.loads(
        text, parse_float=Decimal, object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )
    if not isinstance(value, dict):
        raise ValueError("JSON record must be an object")
    return value


def parse_record(source_name: str, raw_line: str) -> dict[str, Any]:
    """Return parsed storage columns plus the unchanged original JSONL line."""
    if source_name not in SOURCE_COLUMNS:
        raise IngestionError("Unsupported source")
    if "\x00" in raw_line:
        raise IngestionError("Input contains NUL unsupported by PostgreSQL TEXT")
    try:
        raw_line.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise IngestionError("Input cannot be represented as UTF-8 TEXT") from error
    try:
        record = _json_object(raw_line)
    except (ValueError, TypeError, OverflowError, RecursionError, DecimalException):
        record = {}
    parsed = {}
    for column in SOURCE_COLUMNS[source_name]:
        value = record.get(column)
        if column == "amount_cents":
            parsed[column] = parse_amount(record.get("amount_raw"), record.get("currency"))
        elif column in TIME_COLUMNS:
            parsed[column] = parse_timestamp(value)
        else:
            parsed[column] = _text(value)
    parsed["raw_payload"] = raw_line
    return parsed


@dataclass(frozen=True)
class BatchInput:
    source_name: str
    input_reference: str
    scope_complete: bool
    content: bytes

    def __post_init__(self) -> None:
        if self.source_name not in SOURCE_COLUMNS:
            raise IngestionError("Unsupported source")
        if not isinstance(self.input_reference, str) or not self.input_reference or _text(self.input_reference) is None:
            raise IngestionError("Input reference must be nonempty UTF-8 text")
        if type(self.scope_complete) is not bool:
            raise IngestionError("scope_complete must be an explicit boolean")
        if not isinstance(self.content, bytes):
            raise IngestionError("Source content must be bytes")

    @property
    def content_digest(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def batch_id(self) -> str:
        identity = json.dumps(
            ["retail-jsonl-v1", self.source_name, self.input_reference, self.content_digest],
            ensure_ascii=False, separators=(",", ":"),
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    @property
    def raw_lines(self) -> tuple[str, ...]:
        if not self.content:
            return ()
        parts = self.content.split(b"\n")
        records = [part + b"\n" for part in parts[:-1]]
        if parts[-1]:
            records.append(parts[-1])
        try:
            lines = tuple(record.decode("utf-8", errors="strict") for record in records)
        except UnicodeDecodeError as error:
            raise IngestionError("Input is not UTF-8 text") from error
        if any("\x00" in line for line in lines):
            raise IngestionError("Input contains NUL unsupported by PostgreSQL TEXT")
        return lines


def load_manifest(path: str | Path) -> list[BatchInput]:
    """Load all three declared inputs before connecting or writing any batch."""
    manifest_path = Path(path).resolve(strict=True)
    try:
        manifest = _json_object(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise IngestionError("Manifest must be a strict JSON object") from error
    if manifest.get("synthetic") is not True or not isinstance(manifest.get("inputs"), list):
        raise IngestionError("Manifest must label synthetic input and declare inputs")
    root = manifest_path.parent
    batches = []
    sources = set()
    input_paths: set[Path] = set()
    for item in manifest["inputs"]:
        if not isinstance(item, dict) or set(item) != {"source", "path", "scope_complete"}:
            raise IngestionError("Each manifest input needs source, path and scope_complete")
        source = item["source"]
        if not isinstance(source, str) or source not in SOURCE_COLUMNS or source in sources:
            raise IngestionError("Declare each supported source exactly once")
        reference = item["path"]
        if not isinstance(reference, str) or not reference or "\\" in reference or ":" in reference:
            raise IngestionError("Input path must be a relative POSIX path")
        relative = PurePosixPath(reference)
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != reference:
            raise IngestionError("Input path must be canonical and stay under the manifest directory")
        input_path = (root / reference).resolve(strict=True)
        if not input_path.is_relative_to(root) or not input_path.is_file():
            raise IngestionError("Input path must identify a file under the manifest directory")
        if input_path in input_paths:
            raise IngestionError("Independent sources must use distinct input files")
        input_paths.add(input_path)
        batch = BatchInput(source, reference, item["scope_complete"], input_path.read_bytes())
        batch.raw_lines  # Fail unrepresentable UTF-8/NUL before any database writes.
        batches.append(batch)
        sources.add(source)
    if sources != set(SOURCE_COLUMNS):
        raise IngestionError("Manifest must explicitly declare orders, payments and refunds")
    return batches


def _batch_result(batch: BatchInput, count: int, sequence: int, action: str) -> dict[str, Any]:
    return {
        "source_name": batch.source_name, "batch_id": batch.batch_id,
        "content_digest": batch.content_digest, "input_reference": batch.input_reference,
        "row_count": count, "scope_complete": batch.scope_complete,
        "batch_sequence": sequence, "action": action,
    }


def ingest_batch(connection: Any, batch: BatchInput) -> dict[str, Any]:
    """Atomically commit one source batch or verify an unchanged committed batch.

    The caller supplies an idle connection: this function never commits unrelated
    work already in progress. Transactions use an advisory lock for same-identity
    writers. A replay checks persisted metadata and verbatim occurrence lineage;
    it does not parse/recompute previously committed normalized business values.
    """
    if connection.get_transaction_status() != extensions.TRANSACTION_STATUS_IDLE:
        raise IngestionError("ingest_batch requires an idle database connection")
    lines = batch.raw_lines
    batch_id = batch.batch_id
    lock_key = int.from_bytes(bytes.fromhex(batch_id)[:8], byteorder="big", signed=True)
    columns = SOURCE_COLUMNS[batch.source_name] + ("raw_payload",)
    table = sql.Identifier("bronze", batch.source_name + "_source")
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
            cursor.execute(
                "SELECT source_name, content_digest, input_reference, row_count, "
                "scope_complete, status, committed_at, batch_sequence "
                "FROM bronze.input_batches WHERE batch_id=%s FOR UPDATE",
                (batch_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                expected = (batch.source_name, batch.content_digest, batch.input_reference, len(lines), batch.scope_complete)
                if existing[:5] != expected or existing[5] != "COMMITTED" or existing[6] is None:
                    raise BatchConflictError("Existing batch identity conflicts with metadata or state")
                for source_name in SOURCE_COLUMNS:
                    cursor.execute(
                        sql.SQL("SELECT source_row_number, raw_payload FROM {} WHERE batch_id=%s ORDER BY source_row_number").format(
                            sql.Identifier("bronze", source_name + "_source")
                        ), (batch_id,),
                    )
                    actual_rows = cursor.fetchall()
                    expected_rows = list(enumerate(lines, start=1)) if source_name == batch.source_name else []
                    if actual_rows != expected_rows:
                        raise BatchConflictError("Existing committed batch occurrence content or lineage conflicts")
                result = _batch_result(batch, len(lines), existing[7], "UNCHANGED")
            else:
                cursor.execute(
                    "INSERT INTO bronze.input_batches "
                    "(batch_id, source_name, content_digest, input_reference, row_count, scope_complete, status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'REGISTERED') RETURNING batch_sequence",
                    (batch_id, batch.source_name, batch.content_digest, batch.input_reference, len(lines), batch.scope_complete),
                )
                sequence = cursor.fetchone()[0]
                insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    table, sql.SQL(",").join(map(sql.Identifier, ("batch_id", "source_row_number") + columns)),
                    sql.SQL(",").join(sql.Placeholder() for _ in range(2 + len(columns))),
                )
                for row_number, line in enumerate(lines, start=1):
                    parsed = parse_record(batch.source_name, line)
                    cursor.execute(insert, (batch_id, row_number) + tuple(parsed[column] for column in columns))
                cursor.execute(
                    "UPDATE bronze.input_batches SET status='COMMITTED', committed_at=clock_timestamp() WHERE batch_id=%s",
                    (batch_id,),
                )
                result = _batch_result(batch, len(lines), sequence, "COMMITTED")
    return result


def connect_from_env() -> Any:
    """Use local environment credentials; never include connection data in output."""
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "postgres"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "retail"),
        user=os.environ.get("PGUSER", "retail"),
        password=os.environ["RETAIL_DB_PASSWORD"],
        connect_timeout=10, application_name="retail_synthetic_ingestion",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest three synthetic JSONL sources into Retail Bronze")
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args(argv)
    try:
        batches = load_manifest(args.manifest)
        connection = connect_from_env()
        try:
            for batch in batches:
                print(json.dumps(ingest_batch(connection, batch), ensure_ascii=False, sort_keys=True))
        finally:
            connection.close()
        return 0
    except Exception as error:
        # Database errors can contain row values, connection details or credentials.
        # Report only the exception class; do not serialize their messages/DSN.
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
