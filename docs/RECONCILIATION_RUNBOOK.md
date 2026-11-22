# Reconciliation thin slice — M1-T03

This manual local workflow supports F01 (matched payment), F03 (missing payment after due) and F08 (valid partial refund). It uses the existing eight Retail tables and pinned Python/dbt/PostgreSQL runtime. All fixture data is synthetic. It does not establish the full F01–F13 oracle or AC-01–AC-10 completion.

## Prerequisites and execution boundary

Use PowerShell 7 in `D:\retail-order-to-cash\Retail-Order-to-Cash`, on the branch authorized for the task. Complete the [foundation runbook](LOCAL_RUNBOOK.md) first for a fresh environment. Retain the ignored `.env` and existing named volumes. The M1-T01 schema must already exist; M1-T03 does not add tables or alter it.

```powershell
Set-Location 'D:\retail-order-to-cash\Retail-Order-to-Cash'
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120 postgres
# Build only if the existing pinned shared image is absent locally.
docker compose --profile tools build reconcile
docker compose --profile tools run --rm dbt debug
```

Check `$LASTEXITCODE` after each native command and stop on an unexpected nonzero result. The `reconcile` tools-profile runner uses the existing `retail-m1-t01-dbt:1.10.15-postgres1.9.1` image. It runs on demand, publishes no ports and mounts source/configuration read-only. No continuously running reconciliation service, new dependency, database, business Airflow DAG or schedule is introduced.

`dbt/models/thin_reconciliation.sql` owns the monetary calculations and bounded classification. Python validates context, invokes [dbt compilation](https://docs.getdbt.com/reference/commands/compile), then executes the compiled SELECT and publishes Core rows. The model is [ephemeral](https://docs.getdbt.com/docs/build/materializations#ephemeral); private temporary compilation targets/logs are removed when compilation finishes. Python does not independently calculate O, P, R or N.

## Load the independent scenario inputs

Keep `fixtures/synthetic/` unchanged: that M1-T02 ingestion dataset intentionally includes invalid and unknown-status evidence. The new `fixtures/reconciliation/manifest.json` declares three distinct files with explicit `scope_complete: true`, using the approved ingestion interface:

```powershell
docker compose --profile tools run --rm ingest -m retail_ingestion --manifest fixtures/reconciliation/manifest.json
$LASTEXITCODE
```

The scenario files contain four Orders occurrences, five Payments occurrences and one Refund occurrence. The three order IDs are `syn-t03-f01`, `syn-t03-f03` and `syn-t03-f08`. F01 deliberately includes two equivalent order versions and three equivalent payment occurrences covering two payment IDs but one `external_charge_ref`. They must represent one actual 10000-cent charge. Re-ingesting identical files returns `UNCHANGED` under the existing batch-ingestion contract.

Independently authored expected results for `evaluation_as_of = 2026-02-03T00:00:00Z` are:

| Scenario / order ID | O | P | R | N | Overall status | Findings |
|---|---:|---:|---:|---:|---|---|
| F01 / `syn-t03-f01` | 10000 | 10000 | 0 | 10000 | `MATCHED` | None |
| F03 / `syn-t03-f03` | 4500 | 0 | 0 | 0 | `EXCEPTION` | Exactly `MISSING_PAYMENT` |
| F08 / `syn-t03-f08` | 8000 | 8000 | 2000 | 6000 | `MATCHED` | None |

All amounts are integer USD cents. F03 has a PENDING payment that contributes zero effective P. Every scenario order is due at `2026-02-02T00:00:00Z`. F03 is `PENDING` with no Missing Payment finding before and at that instant; the missing-payment condition requires a strictly later evaluation. F08 does not compare N directly with O as the only correctness criterion: its valid completed partial refund reduces N without creating Amount Mismatch or Refund Mismatch.

## Capture and pass an explicit evaluation context

Capture the current committed sequence cutoff after ingestion. The single-quoted shell command references the container's existing password variable; it does not put its value in command text:

```powershell
$cutoff = docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -At "$@"' -- -c "SELECT COALESCE(max(batch_sequence),0) FROM bronze.input_batches WHERE status='COMMITTED';"
if ($LASTEXITCODE -ne 0) { throw 'Could not capture committed cutoff.' }
$cutoff = $cutoff.Trim()
if ($cutoff -notmatch '^[0-9]+$') { throw 'Expected an explicit numeric cutoff.' }

$before = docker compose --profile tools run --rm reconcile -m retail_reconciliation --snapshot-cutoff $cutoff --evaluation-as-of 2026-02-01T23:59:59Z --rules-version m1-thin-v1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Before-due evaluation failed.' }
$at = docker compose --profile tools run --rm reconcile -m retail_reconciliation --snapshot-cutoff $cutoff --evaluation-as-of 2026-02-02T00:00:00Z --rules-version m1-thin-v1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'At-due evaluation failed.' }
$after = docker compose --profile tools run --rm reconcile -m retail_reconciliation --snapshot-cutoff $cutoff --evaluation-as-of 2026-02-03T00:00:00Z --rules-version m1-thin-v1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'After-due evaluation failed.' }

$after.results | Where-Object order_id -like 'syn-t03-*' | Select-Object order_id,expected_cents,payments_cents,refunds_cents,net_cents,overall_status | Format-Table
```

These are different evaluation contexts using the same explicit cutoff. The CLI returns a run ID, stable evaluation key, canonical context, action and SQL-calculated rows with lineage. It can return other known orders already present in the selected database snapshot; the display above selects only the new scenario IDs.

| Input | Meaning |
|---|---|
| `snapshot_cutoff` | Nonnegative BIGINT sequence bound. Only COMMITTED batches at or below it are eligible; a cutoff beyond the currently committed maximum is rejected. Zero represents an empty source snapshot. |
| `evaluation_as_of` | Explicit-offset timestamp normalized to UTC with six fractional digits. Source event/update visibility and financial-effective time are evaluated against it. |
| `rules_version` | Exactly `m1-thin-v1` for this implementation. Unsupported versions are rejected. |

The key is lowercase SHA-256 of compact UTF-8 JSON for `["retail-evaluation-v1", snapshot_cutoff, canonical_UTC_as_of, rules_version]`. Equivalent offset representations of the same instant produce the same key. No run attempt ID, compilation timestamp or ingestion timestamp enters this key.

## Query canonical results, findings and source evidence

Pass the actual keys returned above to the read-only SQL demonstration:

```powershell
$afterKey = $after.evaluation_key
$beforeKey = $before.evaluation_key
docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 "$@" -f /workspace/sql/demo_reconciliation.sql' -- -v "evaluation_key=$afterKey" -v "comparison_key=$beforeKey"
$LASTEXITCODE
```

`sql/demo_reconciliation.sql` shows stored O/P/R/N, overall statuses, aggregated reason codes, attempt/context metadata, before/after status changes and source-record references. The result query keeps one row per order even when findings are joined. The comparison does not reimplement classification or money calculations.

Missing Payment findings persist a flat `evidence_refs` JSON array containing source/table identity, row ID, batch ID, source-row number, source-record ID, source version and relevant business IDs. The demonstration joins those references back to real Bronze occurrences. The existing canonical results table has no lineage column for MATCHED rows: complete model lineage is available in CLI output, and the demonstration separately lists all related visible source occurrences for matched orders. That list is provenance, not another effective-state calculation.

## Replay and publication behavior

An evaluation gets a unique run ID. The writer holds a session advisory lock for its evaluation key before opening the worker's PostgreSQL [REPEATABLE READ snapshot](https://www.postgresql.org/docs/16/transaction-iso.html). Source reads, canonical result/finding inserts and successful attempt status share one publication transaction. Ordinary compilation/publication failure rolls back canonical changes and records the registered attempt as FAILED with a sanitized error class. A retry uses a new run ID.

Replaying identical context compares all stored canonical result and finding fields against the SQL output. It returns `UNCHANGED` when they agree, without updating canonical rows. A prior successful attempt also distinguishes an empty canonical publication from an incomplete one. Conflicting stored context or canonical output is rejected, not overwritten. Context conflicts detected before registration do not append contradictory attempt metadata.

The same explicit CLI command can be replayed twice:

```powershell
$replay1 = docker compose --profile tools run --rm reconcile -m retail_reconciliation --snapshot-cutoff $cutoff --evaluation-as-of 2026-02-03T00:00:00Z --rules-version m1-thin-v1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'First replay failed.' }
$replay2 = docker compose --profile tools run --rm reconcile -m retail_reconciliation --snapshot-cutoff $cutoff --evaluation-as-of 2026-02-03T00:00:00Z --rules-version m1-thin-v1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Second replay failed.' }
$after.evaluation_key, $replay1.evaluation_key, $replay2.evaluation_key
$replay1.action, $replay2.action
```

The three keys should agree and the two replay actions should be `UNCHANGED`. Re-run the SQL demonstration to see identical canonical values and separate successful attempt rows. The smoke harness additionally reads complete stored Core result/finding snapshots before and after both replays and compares them, rather than relying only on CLI messages.

## Verification and evidence

```powershell
docker compose --profile tools run --rm reconcile -m unittest discover -s tests/reconciliation -p test_unit.py -v
docker compose --profile tools run --rm reconcile -m unittest discover -s tests/reconciliation -p test_postgres.py -v
pwsh -NoProfile -File tests/reconciliation_smoke.ps1
$LASTEXITCODE
```

The unit suite validates pure context/key rules and the authored fixture contract. Native integration uses real PostgreSQL and dbt compilation for scenario values, due boundaries, duplicate representations, ambiguity, replay, fault rollback, context conflicts and historical visibility. Fault hooks intentionally inject real SQL errors or concurrent changes to test-owned synthetic rows; no database mock establishes runtime correctness.

The smoke harness creates a new directory under ignored `artifacts/evidence/m1-t03/`, records exact commands/exits/output, ingests scenario inputs, evaluates before/at/after due, verifies two canonical replays and executes the SQL report. It refuses an existing evidence directory. The PR description/evidence comments contain actual results; this runbook makes no unexecuted PASS claim. For M1-T01/T02 compatibility, run their unchanged harnesses with new evidence directories as described in the existing runbooks. Never delete volumes or reset unrelated data to make tests pass.

## Deliberately bounded rules and deferred work

- Readiness requires all three source names to be represented in committed batches within the cutoff, every such batch to declare `scope_complete=true`, matching raw row counts and no wrong-source child rows. An earlier incomplete batch is not silently overridden by a later complete one. Failure of this conservative check prevents MATCHED and Missing Payment classification.
- Known future event/source-update records are excluded from an earlier as-of context. Related records with missing/invalid timestamps, money, identities or unknown statuses make the relevant order `UNDETERMINED`; they do not silently contribute valid P or R. Financially effective SUCCEEDED/COMPLETED rows require an effective timestamp at or before as-of.
- Equivalent business-effective representations collapse across logical IDs and shared charge references. Material conflicts remain `UNDETERMINED`; neither lexical source versions nor ingestion time selects a winner. Raw occurrences are retained. Unassignable records with no link to a known order remain Bronze for future DQ and are not counted as valid money.
- This slice supports positive confirmed obligations and positive payment/refund representations. Unsupported states, full refunds and incomplete mismatch/duplicate investigations stay `UNDETERMINED`, without invented mismatch findings. No general source-version precedence, complete F01–F13 classifications, six-dimension DQ reporting, late-arrival/replay/backfill engine, Gold model or dashboard is implemented.
- Same-key serialization and repeatable reads protect this evaluation workflow. They do not create a permanent physical copy of all source data or prevent arbitrary database-owner SQL edits. A later commitment at a lower sequence or direct mutation can change the recomputed snapshot; conflicting canonical output is rejected on replay. General historical source-state reconstruction is deferred to M2.
- A hard process/database interruption can leave an attempt RUNNING; that state is not a successful canonical publication. This task does not add a crash-recovery scheduler or production monitoring framework.

There is no merge, deployment, cloud provisioning, external publication or next-task authorization in these commands. Task handoff belongs in the GitHub PR description and evidence comments; no standalone TASK REPORT or ZIP is created.
