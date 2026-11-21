# Synthetic three-source ingestion — M1-T02

Use the existing [foundation runtime](LOCAL_RUNBOOK.md), PowerShell 7 and the branch authorized for this task. All identifiers and records in `fixtures/synthetic/` are synthetic. These ingestion fixtures do not define F01–F13 reconciliation outcomes or prove AC-01–AC-10.

## Run and verify

From `D:\retail-order-to-cash\Retail-Order-to-Cash`, retain the existing ignored `.env` and named PostgreSQL volume. The M1-T01 schema must already exist. For a fresh environment, complete the foundation runbook first.

```powershell
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120 postgres
# Build only if the pinned shared Python/dbt image is not present locally.
docker compose --profile tools build ingest
docker compose --profile tools run --rm ingest
$LASTEXITCODE
```

`ingest` is a disposable Python command, using the existing pinned dbt image and its `psycopg2-binary==2.9.13`. It has no public ports, schedule, persistent service or new database. Source, fixtures and tests are mounted read-only. The default command reads `fixtures/synthetic/manifest.json` and prints one safe JSON summary per source. Credentials come from the existing environment and are never printed. A fatal CLI error reports its exception class and exit 1; source-row and connection details are excluded from the error output.

Run independent unit and native PostgreSQL tests:

```powershell
docker compose --profile tools run --rm ingest -m unittest discover -s tests/ingestion -p test_unit.py -v
docker compose --profile tools run --rm ingest -m unittest discover -s tests/ingestion -p test_postgres.py -v
```

Or record command arguments, expected/actual exits and complete output for both suites, three CLI attempts and foundation SQL compatibility:

```powershell
pwsh -NoProfile -File tests/ingestion_smoke.ps1
$LASTEXITCODE
```

The smoke script creates a new timestamped directory under ignored `artifacts/evidence/m1-t02/` and refuses an existing evidence-directory argument. A failure stops dependent steps; no unexecuted check is marked PASS. Unit tests run in Python without database assertions; `test_postgres.py` runs against real PostgreSQL and does not substitute a database mock. Its actual SQL division-by-zero injection after a successful occurrence insert must roll back that entire batch. Native tests use unique synthetic references and clean only their own rows. The three CLI attempts ingest the shipped dataset and leave it available locally; replaying them is safe. For full foundation runtime regression use the unchanged `tests/runtime_smoke.ps1` as documented in the foundation runbook.

## Manifest and authored fixture ledger

The manifest labels the dataset synthetic and declares exactly one input for each of `orders`, `payments`, `refunds`. Every entry must contain `source`, a canonical relative POSIX `path`, and an explicit boolean `scope_complete`. Paths resolve beneath the manifest directory. File existence never establishes completeness. The original relative path string is stored as `input_reference`; an absolute mount path or file modification time is not identity.

| Source | Input reference | Intended occurrences | Representative evidence |
|---|---|---:|---|
| Orders | `orders.jsonl` | 6 | Paid/unpaid/refunded order inputs, repeated order/version, invalid money/status/time, malformed JSON line |
| Payments | `payments.jsonl` | 6 | SUCCEEDED and PENDING inputs, repeated payment/version, orphan order, invalid money/status/time |
| Refunds | `refunds.jsonl` | 4 | Completed partial refund, orphan references, invalid money/status/time, blank physical line |

Expected storage after the first successful CLI execution is three COMMITTED batches and 16 raw occurrences (6/6/4), including the malformed/blank lines. Subsequent identical inputs return `UNCHANGED`; they do not add batches/rows or update existing values/timestamps. No order/payment/refund classification or financial-effective sum is asserted here.

## Representation and identity

Input is bounded UTF-8 JSON Lines, split only on LF. Raw payload retains every physical line's original characters and terminator, including CRLF, blank lines and a final unterminated line. An empty file has zero occurrences and can explicitly declare empty scope readiness. JSONL line-ending changes change the input digest. Git pins fixture JSONL to LF for reproducible checkouts.

`content_digest` is the lowercase SHA-256 hex of the entire original file bytes. `batch_id` is SHA-256 of UTF-8 JSON for this ordered array, with `ensure_ascii=False` and compact separators `(',', ':')`:

```text
["retail-jsonl-v1", source_name, input_reference, content_digest]
```

`scope_complete` is checked metadata, not part of the identity. Reusing an identity with changed completeness fails rather than rewriting a committed batch. A changed file yields a different batch identity and preserves its source occurrences independently; it does not add monetary amounts or choose a source-version winner.

Supported text fields require source strings. Numeric/boolean/object values are not silently coerced; the full original line remains available. Unknown statuses stay as raw source strings. Malformed JSON, duplicate JSON keys, non-object JSON, blank lines and nonstandard numeric constants keep their raw line with NULL parsed business columns. Unknown extra fields remain in raw payload. Wrong-type and malformed field values leave only their normalized fields NULL where the JSON object is otherwise valid.

The existing PostgreSQL TEXT representation cannot contain actual NUL characters or invalid UTF-8 bytes. Such files fail explicitly before database writes; bytes are never dropped, substituted or re-encoded into invented source records. Escaped JSON NUL/unpaired surrogate values leave unsupported parsed text fields NULL while the original ASCII line survives. No broader binary-file ingestion format is introduced.

## Exact money and timestamps

Money normalization accepts USD source strings matching ASCII `[+-]?[0-9]+\.[0-9]{2}` and signed BIGINT cents range. It does not trim, use binary float, accept exponent notation, or round extra decimals. Signs and leading zeros are preserved in `amount_raw`; negative values are source evidence for later business validation, not an invented refund/payment rule. Invalid representations, non-string amounts and non-USD currency yield NULL cents. Example independent expectations: `100.00` → 10000, `20.00` → 2000, `12.345` → NULL; BIGINT overflow → NULL.

Validated timestamps require `YYYY-MM-DDTHH:MM:SS`, optional 1–6 fractional digits, and explicit `Z` or `±HH:MM`. Calendar/offset bounds are checked, then values are normalized to UTC. Missing/naive/invalid timestamps and excess fractional precision stay NULL without losing the source line. `source_version` stays a string; ingestion time never determines update precedence.

## Transaction, replay and limits

One database transaction writes one source batch: REGISTERED metadata, all intended raw occurrences and COMMITTED status/commit time. A fatal failure rolls back that batch's metadata and rows. All manifest files are loaded/validated for representability before connecting; the three batches remain separate transactions. A later batch's database failure does not undo earlier successfully committed independent batches.

An advisory transaction lock serializes writers of the same deterministic identity. Replay verifies persisted metadata, status, row count, exact ordered raw lines, row numbers and absence of rows in the wrong source table. It returns `UNCHANGED` without recalculating parsed business values. Conflicting/tampered batches fail safely; the workflow does not repair or rewrite them. Native tests compare complete before/after snapshots including row IDs, sequence numbers and timestamps for two repeat ingestions and exercise concurrent same-batch writers.

These protections apply to this ingestion workflow. The Retail database owner can still change tables directly; no database-wide immutability trigger or authorization framework is claimed. No extra table, business DQ findings, effective-state selection, Silver/Gold model, business Airflow DAG, replay/backfill workflow or external publication was added. Existing core tables are untouched.

Use `docker compose down` to stop containers while retaining named volumes when finished. Do not reset the database, remove volumes or delete unrelated evidence. The PR description contains actual execution results, commit SHA and remaining review limitations; no replacement task-report file is created.
