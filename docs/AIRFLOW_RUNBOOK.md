# Airflow end-to-end thin slice — M1-T04

This manually triggered local DAG runs the existing M1-T02 ingestion and M1-T03 reconciliation workflow against synthetic F01, F03 and F08 inputs. It demonstrates orchestration through the real Airflow scheduler and LocalExecutor. It does not establish full reconciliation, complete DQ, historical backfill or production scheduling.

## Runtime and compatibility boundary

Use PowerShell 7 in `D:\retail-order-to-cash\Retail-Order-to-Cash`, on the branch authorized for the task. For a fresh environment, complete the [foundation setup](LOCAL_RUNBOOK.md) first. Keep the ignored `.env`, named database volumes and existing source records. The existing eight Retail tables must already exist.

The Airflow image extends the approved `apache/airflow:2.11.0-python3.12` image with an isolated Python virtual environment containing the unchanged `docker/dbt/requirements.txt` pins, including dbt Core 1.10.15 and dbt-postgres 1.9.1. It installs the same pinned Git dependency used by the existing dbt image. Airflow's own packages remain separate from the dbt dependencies. Standard `ExternalPythonOperator` tasks invoke that interpreter; the reconciliation subprocess finds `dbt` in the venv's `bin` directory. Dependencies are installed when building the image, not during a DAG run.

The Compose runtime retains LocalExecutor, parallelism 2, active tasks per DAG 2 and active runs per DAG 1. PostgreSQL remains version 16 with separate Retail and Airflow metadata databases/users. Source, fixture, DAG and dbt mounts are read-only. No Docker socket, privileged container access or additional execution service is needed. The Retail password comes from the private container environment; it is not a DAG argument or XCom value.

```powershell
Set-Location 'D:\retail-order-to-cash\Retail-Order-to-Cash'
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 180 postgres airflow-scheduler airflow-webserver
docker compose ps -a
docker compose exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob
docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json
```

Check `$LASTEXITCODE` after every native command and stop on an unexpected nonzero exit. Wait for a healthy scheduler/webserver and for `retail_m1_thin_slice` to appear. `list-import-errors` must report no errors. An image build does not reset PostgreSQL or replace existing volumes.

## Locate and manually trigger the DAG

Open [the local Airflow UI](http://localhost:8080). Sign in as `admin` using the private `AIRFLOW_ADMIN_PASSWORD` from `.env`; do not paste credentials into commands, screenshots or review evidence. Locate `retail_m1_thin_slice` and unpause it. The DAG has `schedule=None`, `catchup=False` and `max_active_runs=1`; unpausing does not establish an automatic schedule.

Use the UI's trigger action with empty configuration for a normal run, or the CLI:

```powershell
$runId = 'manual-m1-t04-' + [guid]::NewGuid().ToString('N')
docker compose exec -T airflow-scheduler airflow dags unpause retail_m1_thin_slice
docker compose exec -T airflow-scheduler airflow dags trigger retail_m1_thin_slice --run-id $runId
docker compose exec -T airflow-scheduler airflow dags list-runs --dag-id retail_m1_thin_slice --output json
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run retail_m1_thin_slice $runId --output json
```

Triggering queues work; it does not wait for completion. Repeat the last two read-only commands or inspect the UI until the run reaches a terminal state. Require a successful DAG run and SUCCESS for all three tasks, in this dependency order:

1. `ingest_sources` loads `fixtures/reconciliation/manifest.json` through the existing ingestion module: Orders, Payments and Refunds, atomic per-source batches, deterministic batch identity and raw lineage. Existing identical batches are reused as `UNCHANGED`.
2. `capture_snapshot` queries the maximum currently COMMITTED Retail batch sequence and validates a nonnegative BIGINT. Its return value is the only XCom: an integer cutoff.
3. `reconcile_snapshot` passes that cutoff, the explicit timestamp `2026-02-03T00:00:00Z` and rules version `m1-thin-v1` to the existing reconciliation module and dbt SQL model. The business timestamp is fixed in the DAG; no trigger configuration, system clock or Airflow logical date supplies it.

The business calculations remain in the existing SQL model. Airflow does not calculate money, choose source-version winners or infer completeness from files. Ingestion and reconciliation do not publish their result payloads to XCom. Task logs contain synthetic source batch outcomes and the reconciliation summary, including its evaluation key; errors propagate as failed tasks with sanitized exception classes.

## Inspect results and source evidence

Copy the 64-character lowercase hexadecimal `evaluation_key` from the successful `reconcile_snapshot` task log. Use the existing read-only SQL demonstration:

```powershell
$evaluationKey = '<evaluation_key from the successful task log>'
if ($evaluationKey -notmatch '^[a-f0-9]{64}$') { throw 'Expected the actual evaluation key.' }
docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 "$@" -f /workspace/sql/demo_reconciliation.sql' -- -v "evaluation_key=$evaluationKey"
$LASTEXITCODE
```

The shell references the existing container password variable without putting its value in command text. The report displays stored O/P/R/N, one row per order with aggregated finding codes, evaluation attempts, exception source references and matched-order provenance. Other synthetic orders may already exist in the same snapshot. The independent expectations for the three demonstration IDs are:

| Scenario / order ID | O | P | R | N | Overall status | Findings |
|---|---:|---:|---:|---:|---|---|
| F01 / `syn-t03-f01` | 10000 | 10000 | 0 | 10000 | MATCHED | None |
| F03 / `syn-t03-f03` | 4500 | 0 | 0 | 0 | EXCEPTION | Exactly MISSING_PAYMENT |
| F08 / `syn-t03-f08` | 8000 | 8000 | 2000 | 6000 | MATCHED | None |

All money is USD integer cents. F01's equivalent versions and payment aliases represent one real charge. F03 is evaluated strictly after its payment due time with complete inputs; its PENDING payment contributes zero effective money. F08's completed 2000-cent partial refund reduces net money to 6000 while preserving MATCHED.

Readiness remains conservative: every eligible source batch must be COMMITTED, scope-complete and consistent with raw rows, and all three source names must be represented. If existing source data prevents these expected results, preserve it and stop for review. Do not remove batches, alter source records or reset volumes to make the demonstration pass.

## Trigger two unchanged replays

After the first run succeeds, trigger the same DAG twice more, waiting for each run to finish before triggering the next:

```powershell
$replay1 = 'manual-m1-t04-replay1-' + [guid]::NewGuid().ToString('N')
docker compose exec -T airflow-scheduler airflow dags trigger retail_m1_thin_slice --run-id $replay1
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run retail_m1_thin_slice $replay1 --output json
# Wait until replay1 succeeds before continuing.
$replay2 = 'manual-m1-t04-replay2-' + [guid]::NewGuid().ToString('N')
docker compose exec -T airflow-scheduler airflow dags trigger retail_m1_thin_slice --run-id $replay2
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run retail_m1_thin_slice $replay2 --output json
```

With unchanged source state, all three ingestion actions and reconciliation actions should be `UNCHANGED`; cutoff and evaluation key remain identical. Source batches/raw occurrences and canonical results/findings remain unchanged. Each successful reconciliation execution appends a distinct run attempt under the stable evaluation key; these attempt rows are expected history, not additional business transactions. The native smoke harness compares complete persisted business-state snapshots before and after the two replays, rather than relying only on log messages or row counts.

## Diagnose a controlled failure and retry safely

The local demonstration supports a strict boolean `fail_reconcile_once` trigger configuration. It deliberately raises a sanitized error before reconciliation on the first task try. Normal runs omit it. To exercise failure:

```powershell
$failureRun = 'manual-m1-t04-failure-' + [guid]::NewGuid().ToString('N')
docker compose exec -T airflow-scheduler airflow dags trigger retail_m1_thin_slice --run-id $failureRun --conf '{"fail_reconcile_once":true}'
docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run retail_m1_thin_slice $failureRun --output json
```

Wait until the DAG is FAILED. Verify ingestion and snapshot tasks succeeded and `reconcile_snapshot` failed. Inspect its first-try UI log and preserve the run ID, task state and sanitized error. The failure occurs before the reconciliation writer, so it creates no financial writes or new Retail evaluation attempt. It demonstrates scheduler failure propagation; the separate reconciliation regressions exercise publication rollback.

In the UI, select only the failed `reconcile_snapshot` task instance for this run and choose **Clear**. Keep upstream, downstream, past and future selections disabled, and ensure the failed DagRun is reactivated for the scheduler. Do not mark the task successful manually. An equivalent authenticated local API request is:

```text
POST /api/v1/dags/retail_m1_thin_slice/clearTaskInstances
Content-Type: application/json
```

```json
{
  "dag_run_id": "<the actual failureRun ID>",
  "task_ids": ["reconcile_snapshot"],
  "dry_run": false,
  "only_failed": true,
  "reset_dag_runs": true,
  "include_upstream": false,
  "include_downstream": false,
  "include_past": false,
  "include_future": false
}
```

Use private local authentication; never include its header in evidence. Confirm that the response selects exactly this one task instance. The scheduler reruns reconciliation as try 2, retaining the successful snapshot task and its original cutoff XCom. Require the same DagRun and all three tasks to finish successfully. Query the SQL report again and compare unchanged business state. Clearing a failed task preserves source data and retry history; no destructive cleanup is needed. For other failures, diagnose their actual sanitized class and runtime logs before retrying; the controlled-failure switch does not bypass real errors.

## Native verification and review evidence

Run relevant regressions sequentially with new evidence directories, followed by the Airflow harness:

```powershell
$evidenceBase = 'artifacts/evidence/m1-t04/manual-' + [guid]::NewGuid().ToString('N')
pwsh -NoProfile -File tests/runtime_smoke.ps1 -EvidenceDirectory ($evidenceBase + '-t01')
if ($LASTEXITCODE -ne 0) { throw 'Foundation regression failed; preserve its evidence.' }
pwsh -NoProfile -File tests/ingestion_smoke.ps1 -EvidenceDirectory ($evidenceBase + '-t02')
if ($LASTEXITCODE -ne 0) { throw 'Ingestion regression failed; preserve its evidence.' }
pwsh -NoProfile -File tests/reconciliation_smoke.ps1 -EvidenceDirectory ($evidenceBase + '-t03')
if ($LASTEXITCODE -ne 0) { throw 'Reconciliation regression failed; preserve its evidence.' }
pwsh -NoProfile -File tests/airflow_smoke.ps1 -EvidenceDirectory ($evidenceBase + '-native')
if ($LASTEXITCODE -ne 0) { throw 'Airflow verification failed; preserve its evidence.' }
```

The generated suffix keeps each evidence directory new; existing evidence must remain intact. Do not run regression harnesses concurrently with DAG demonstrations: the foundation harness temporarily creates its own schema-test sentinel and restarts services, while reconciliation tests create scoped synthetic records before cleaning up their own fixtures.

The Airflow harness records commands/exits, parse/configuration checks, actual manually triggered scheduler runs and task logs, independent scenario assertions, two full-state replay comparisons, controlled failure/retry, SQL reporting and the eight-table contract. Database-isolation evidence comes from the separate foundation regression above. `airflow dags test`, unit tests and parsing alone do not demonstrate scheduler/LocalExecutor execution. Raw evidence remains under ignored `artifacts/evidence/`; sanitized excerpts and actual results belong in the task PR for independent review. This runbook makes no unexecuted PASS claim and requires no standalone TASK REPORT or evidence ZIP.

Keep the DAG manually triggered. Full scenario coverage, general version precedence, broad backfill, complete DQ, production monitoring, external publishing and deployment remain outside M1-T04. Review, merge and any next task require their separate authorization.
